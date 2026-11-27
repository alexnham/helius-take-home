//! Convert Jetstreamer's transaction type into the decoder's `Tx`.
use common::decode::{Instruction, TokenBalance, Tx};
use jetstreamer_firehose::firehose::TransactionData;

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
