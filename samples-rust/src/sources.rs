//! Convert each source's transaction type into the decoder's `Tx`.
use crate::decode::{Instruction, TokenBalance, Tx};
use helius_laserstream::grpc::SubscribeUpdateTransactionInfo;
use jetstreamer_firehose::firehose::TransactionData;
use serde_json::Value;

fn b58(bytes: impl AsRef<[u8]>) -> String {
    bs58::encode(bytes).into_string()
}

/// LaserStream gRPC: protobuf bytes.
pub fn from_laserstream(info: &SubscribeUpdateTransactionInfo) -> Option<Tx> {
    let message = info.transaction.as_ref()?.message.as_ref()?;
    let meta = info.meta.as_ref()?;
    let ix = |program_id_index: u32, accounts: &[u8], data: &[u8]| Instruction {
        program_id_index: program_id_index as usize,
        accounts: accounts.to_vec(),
        data: data.to_vec(),
    };
    Some(Tx {
        signature: b58(&info.signature),
        failed: meta.err.is_some(),
        keys: message.account_keys.iter()
            .chain(&meta.loaded_writable_addresses)
            .chain(&meta.loaded_readonly_addresses)
            .map(b58)
            .collect(),
        instructions: message.instructions.iter().map(|i| ix(i.program_id_index, &i.accounts, &i.data)).collect(),
        inner_instructions: meta.inner_instructions.iter()
            .map(|g| (g.index as usize, g.instructions.iter().map(|i| ix(i.program_id_index, &i.accounts, &i.data)).collect()))
            .collect(),
        token_balances: meta.pre_token_balances.iter().chain(&meta.post_token_balances)
            .map(|b| TokenBalance { account_index: b.account_index as usize, mint: b.mint.clone(), owner: b.owner.clone() })
            .collect(),
    })
}

/// RPC getBlock with `encoding: "json"`: base58 strings and JSON numbers.
pub fn from_rpc(tx: &Value) -> Option<Tx> {
    let message = &tx["transaction"]["message"];
    let meta = &tx["meta"];
    let strings = |v: &Value| v.as_array().into_iter().flatten().filter_map(|s| s.as_str().map(String::from)).collect::<Vec<_>>();
    let ix = |i: &Value| -> Option<Instruction> {
        Some(Instruction {
            program_id_index: i["programIdIndex"].as_u64()? as usize,
            accounts: i["accounts"].as_array()?.iter().filter_map(|a| a.as_u64().map(|a| a as u8)).collect(),
            data: bs58::decode(i["data"].as_str()?).into_vec().ok()?,
        })
    };
    let list = |v: &Value| v.as_array().map(|a| a.iter().filter_map(ix).collect()).unwrap_or_default();
    let balances = meta["preTokenBalances"].as_array().into_iter().chain(meta["postTokenBalances"].as_array()).flatten();
    Some(Tx {
        signature: tx["transaction"]["signatures"][0].as_str()?.into(),
        failed: !meta["err"].is_null(),
        keys: [strings(&message["accountKeys"]), strings(&meta["loadedAddresses"]["writable"]),
               strings(&meta["loadedAddresses"]["readonly"])].concat(),
        instructions: list(&message["instructions"]),
        inner_instructions: meta["innerInstructions"].as_array().into_iter().flatten()
            .filter_map(|g| Some((g["index"].as_u64()? as usize, list(&g["instructions"]))))
            .collect(),
        token_balances: balances
            .filter_map(|b| Some(TokenBalance {
                account_index: b["accountIndex"].as_u64()? as usize,
                mint: b["mint"].as_str()?.into(),
                owner: b["owner"].as_str().unwrap_or_default().into(),
            }))
            .collect(),
    })
}

/// Old Faithful via Jetstreamer: Agave's native types.
pub fn from_archive(tx: &TransactionData) -> Tx {
    let meta = &tx.transaction_status_meta;
    let message = &tx.transaction.message;
    let ix = |i: &solana_message::compiled_instruction::CompiledInstruction| Instruction {
        program_id_index: i.program_id_index as usize,
        accounts: i.accounts.clone(),
        data: i.data.clone(),
    };
    Tx {
        signature: tx.signature.to_string(),
        failed: tx.is_vote || meta.status.is_err(),
        keys: message.static_account_keys().iter()
            .chain(&meta.loaded_addresses.writable)
            .chain(&meta.loaded_addresses.readonly)
            .map(|k| k.to_string())
            .collect(),
        instructions: message.instructions().iter().map(ix).collect(),
        inner_instructions: meta.inner_instructions.iter().flatten()
            .map(|g| (g.index as usize, g.instructions.iter().map(|i| ix(&i.instruction)).collect()))
            .collect(),
        token_balances: meta.pre_token_balances.iter().flatten().chain(meta.post_token_balances.iter().flatten())
            .map(|b| TokenBalance { account_index: b.account_index as usize, mint: b.mint.clone(), owner: b.owner.clone() })
            .collect(),
    }
}
