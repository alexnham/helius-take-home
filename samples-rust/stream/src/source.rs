//! Convert LaserStream's transaction type into the decoder's `Tx`.
use common::decode::{Instruction, TokenBalance, Tx};
use helius_laserstream::grpc::SubscribeUpdateTransactionInfo;

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
