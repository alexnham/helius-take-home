//! Convert each source's transaction type into the decoder's `Tx`.
use crate::decode::{Instruction, TokenBalance, Tx};
use helius_laserstream::grpc::SubscribeUpdateTransactionInfo;
use jetstreamer_firehose::firehose::TransactionData;
use serde_json::Value;

fn b58(bytes: impl AsRef<[u8]>) -> String {
    bs58::encode(bytes).into_string()
}

/// LaserStream gRPC: protobuf bytes.
/// Errors instead of skipping: a dropped transaction would leave a silent hole in the index.
pub fn from_laserstream(info: &SubscribeUpdateTransactionInfo) -> Result<Tx, String> {
    let signature = b58(&info.signature);
    let message = info.transaction.as_ref().and_then(|t| t.message.as_ref()).ok_or(format!("{signature}: no message"))?;
    let meta = info.meta.as_ref().ok_or(format!("{signature}: no meta"))?;
    let ix = |program_id_index: u32, accounts: &[u8], data: &[u8]| Instruction {
        program_id_index: program_id_index as usize,
        accounts: accounts.to_vec(),
        data: data.to_vec(),
    };
    Ok(Tx {
        signature,
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
/// Every malformed field is an error, never a skip: dropping one account index would shift the rest
/// and attribute a transfer to the wrong account.
pub fn from_rpc(tx: &Value) -> Result<Tx, String> {
    let signature = tx["transaction"]["signatures"][0].as_str().ok_or("transaction without signature")?;
    let bad = |what: &str| format!("{signature}: bad {what}");
    let message = &tx["transaction"]["message"];
    let meta = &tx["meta"];
    if !meta.is_object() {
        return Err(bad("meta"));
    }
    let strings = |v: &Value, what: &str| -> Result<Vec<String>, String> {
        v.as_array().ok_or_else(|| bad(what))?.iter()
            .map(|s| s.as_str().map(String::from).ok_or_else(|| bad(what)))
            .collect()
    };
    // Legacy transactions have no lookup tables.
    let loaded = |kind: &str| match &meta["loadedAddresses"][kind] {
        Value::Null => Ok(vec![]),
        v => strings(v, "loadedAddresses"),
    };
    let ix = |i: &Value| -> Result<Instruction, String> {
        Ok(Instruction {
            program_id_index: i["programIdIndex"].as_u64().ok_or_else(|| bad("programIdIndex"))? as usize,
            accounts: i["accounts"].as_array().ok_or_else(|| bad("accounts"))?.iter()
                .map(|a| a.as_u64().and_then(|a| u8::try_from(a).ok()).ok_or_else(|| bad("account index")))
                .collect::<Result<_, _>>()?,
            data: bs58::decode(i["data"].as_str().ok_or_else(|| bad("data"))?).into_vec().map_err(|_| bad("data"))?,
        })
    };
    let list = |v: &Value| -> Result<Vec<Instruction>, String> {
        v.as_array().ok_or_else(|| bad("instructions"))?.iter().map(ix).collect()
    };
    let balances = |key: &str| meta[key].as_array().ok_or_else(|| bad(key));
    Ok(Tx {
        signature: signature.into(),
        failed: !meta["err"].is_null(),
        keys: [strings(&message["accountKeys"], "accountKeys")?, loaded("writable")?, loaded("readonly")?].concat(),
        instructions: list(&message["instructions"])?,
        inner_instructions: meta["innerInstructions"].as_array().ok_or_else(|| bad("innerInstructions"))?.iter()
            .map(|g| -> Result<_, String> {
                Ok((g["index"].as_u64().ok_or_else(|| bad("inner index"))? as usize, list(&g["instructions"])?))
            })
            .collect::<Result<_, _>>()?,
        token_balances: balances("preTokenBalances")?.iter().chain(balances("postTokenBalances")?)
            .map(|b| -> Result<_, String> {
                Ok(TokenBalance {
                    account_index: b["accountIndex"].as_u64().ok_or_else(|| bad("accountIndex"))? as usize,
                    mint: b["mint"].as_str().ok_or_else(|| bad("mint"))?.into(),
                    owner: b["owner"].as_str().unwrap_or_default().into(), // older balances omit owner
                })
            })
            .collect::<Result<_, _>>()?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rpc_tx(accounts: Value) -> Value {
        json!({
            "transaction": {
                "signatures": ["sig"],
                "message": {
                    "accountKeys": ["signer", "src", "dst", "program"],
                    "instructions": [{ "programIdIndex": 3, "accounts": accounts, "data": "3Bxs4h24hBtQy9rw" }],
                },
            },
            "meta": { "err": null, "innerInstructions": [], "preTokenBalances": [], "postTokenBalances": [] },
        })
    }

    #[test]
    fn rpc_parses_well_formed_transaction() {
        let tx = from_rpc(&rpc_tx(json!([1, 2, 0]))).unwrap();
        assert_eq!(tx.instructions[0].accounts, vec![1, 2, 0]);
        assert!(tx.keys.len() == 4 && !tx.failed);
    }

    #[test]
    fn rpc_rejects_bad_fields_instead_of_dropping_them() {
        // Dropping 300 would shift the destination into the source slot.
        assert!(from_rpc(&rpc_tx(json!([300, 2, 0]))).is_err());
        let mut no_balances = rpc_tx(json!([1, 2, 0]));
        no_balances["meta"].as_object_mut().unwrap().remove("preTokenBalances");
        assert!(from_rpc(&no_balances).is_err());
    }
}
