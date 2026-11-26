//! One decoder for every source. Each source (LaserStream, RPC getBlock, Old Faithful archive)
//! converts its transaction type into a `Tx`, so the same transfer gets the same key on every path.

pub const MINTS: [&str; 2] = [
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
];
const TOKEN_PROGRAMS: [&str; 2] = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", // Token-2022: same Transfer/TransferChecked layout
];
const TRANSFER: u8 = 3;
const TRANSFER_CHECKED: u8 = 12;

#[derive(Debug, PartialEq)]
pub struct Transfer {
    pub signature: String,
    pub ix_index: i32,
    pub inner_ix_index: i32, // -1 for outer instructions
    pub mint: String,
    pub source: String,
    pub destination: String,
    pub source_owner: Option<String>,
    pub destination_owner: Option<String>,
    pub amount: u64,
}

pub struct Instruction {
    pub program_id_index: usize,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

pub struct TokenBalance {
    pub account_index: usize,
    pub mint: String,
    pub owner: String,
}

/// The parts of a transaction the decoder needs, in base58 where Solana tools show base58.
pub struct Tx {
    pub signature: String,
    pub failed: bool,
    /// Static keys, then lookup-table writable, then readonly: the order instructions index into.
    pub keys: Vec<String>,
    pub instructions: Vec<Instruction>,
    /// (outer instruction index, the CPIs it made)
    pub inner_instructions: Vec<(usize, Vec<Instruction>)>,
    pub token_balances: Vec<TokenBalance>, // pre and post
}

impl Tx {
    /// The archive/RPC equivalent of LaserStream's matchMints: does any token balance involve our mints?
    pub fn touches_mints(&self) -> bool {
        self.token_balances.iter().any(|b| MINTS.contains(&b.mint.as_str()))
    }
}

// shortcut: Token-2022 TransferCheckedWithFee, confidential transfers and p-token Batch are skipped.
pub fn decode_transfers(tx: &Tx) -> Vec<Transfer> {
    if tx.failed || !tx.touches_mints() {
        return vec![];
    }
    // Mint and owner of every token account the transaction touched, by address.
    let token_accounts: std::collections::HashMap<&str, &TokenBalance> = tx
        .token_balances
        .iter()
        .filter_map(|b| Some((tx.keys.get(b.account_index)?.as_str(), b)))
        .collect();

    // Outer instructions, then inner ones tagged with the outer index that made them.
    let outer = tx.instructions.iter().enumerate().map(|(i, ix)| (i as i32, -1, ix));
    let inner = tx.inner_instructions.iter().flat_map(|(index, ixs)| {
        ixs.iter().enumerate().map(move |(j, ix)| (*index as i32, j as i32, ix))
    });

    let mut transfers = vec![];
    for (ix_index, inner_ix_index, ix) in outer.chain(inner) {
        if !tx.keys.get(ix.program_id_index).is_some_and(|p| TOKEN_PROGRAMS.contains(&p.as_str())) {
            continue;
        }
        let Some((source, destination, checked_mint, amount)) = parse_transfer(ix, &tx.keys) else { continue };
        let source_info = token_accounts.get(source);
        let destination_info = token_accounts.get(destination);
        // Plain Transfer doesn't name the mint; take it from the token balances of the accounts involved.
        let Some(mint) = checked_mint
            .or(source_info.map(|b| b.mint.as_str()))
            .or(destination_info.map(|b| b.mint.as_str()))
        else {
            continue;
        };
        if !MINTS.contains(&mint) {
            continue; // a swap carries other tokens' transfers too
        }
        let owner = |b: Option<&&TokenBalance>| b.map(|b| b.owner.clone()).filter(|o| !o.is_empty());
        transfers.push(Transfer {
            signature: tx.signature.clone(),
            ix_index,
            inner_ix_index,
            mint: mint.into(),
            source: source.into(),
            destination: destination.into(),
            source_owner: owner(source_info),
            destination_owner: owner(destination_info),
            amount,
        });
    }
    transfers
}

/// Returns (source, destination, mint if the instruction names it, amount).
fn parse_transfer<'a>(ix: &Instruction, keys: &'a [String]) -> Option<(&'a str, &'a str, Option<&'a str>, u64)> {
    let account = |n: usize| ix.accounts.get(n).and_then(|&i| keys.get(i as usize)).map(String::as_str);
    let amount = || Some(u64::from_le_bytes(ix.data.get(1..9)?.try_into().ok()?));
    match (ix.data.first(), ix.data.len()) {
        // Transfer: data [3, u64 amount], accounts [source, destination, authority]
        (Some(&TRANSFER), 9) => Some((account(0)?, account(1)?, None, amount()?)),
        // TransferChecked: data [12, u64 amount, u8 decimals], accounts [source, mint, destination, authority]
        (Some(&TRANSFER_CHECKED), 10) => Some((account(0)?, account(2)?, Some(account(1)?), amount()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USDC: &str = MINTS[0];

    fn data(op: u8, amount: u64, decimals: Option<u8>) -> Vec<u8> {
        [vec![op], amount.to_le_bytes().to_vec(), decimals.into_iter().collect()].concat()
    }

    // keys: 0 signer, 1 src, 2 dst, 3 token program, 4 mint; 5 comes from a lookup table
    fn tx() -> Tx {
        Tx {
            signature: "sig".into(),
            failed: false,
            keys: ["signer", "src", "dst", TOKEN_PROGRAMS[0], USDC, "lookup"].map(String::from).to_vec(),
            instructions: vec![
                Instruction { program_id_index: 3, accounts: vec![1, 2, 0], data: data(TRANSFER, 5_000_000, None) },
                Instruction { program_id_index: 0, accounts: vec![], data: vec![1] }, // not a token program
            ],
            inner_instructions: vec![(1, vec![
                // TransferChecked to the lookup-table account, amount > 2^53
                Instruction { program_id_index: 3, accounts: vec![2, 4, 5, 0], data: data(TRANSFER_CHECKED, 1 << 60, Some(6)) },
            ])],
            token_balances: vec![
                TokenBalance { account_index: 1, mint: USDC.into(), owner: "ownerA".into() },
                TokenBalance { account_index: 5, mint: USDC.into(), owner: "ownerB".into() },
            ],
        }
    }

    #[test]
    fn decodes_outer_transfer_and_inner_transfer_checked() {
        let transfers = decode_transfers(&tx());
        assert_eq!(transfers.len(), 2);
        let (outer, inner) = (&transfers[0], &transfers[1]);
        assert_eq!((outer.ix_index, outer.inner_ix_index, outer.mint.as_str()), (0, -1, USDC));
        assert_eq!((outer.source.as_str(), outer.destination.as_str(), outer.amount), ("src", "dst", 5_000_000));
        assert_eq!((outer.source_owner.as_deref(), outer.destination_owner.as_deref()), (Some("ownerA"), None));
        assert_eq!((inner.ix_index, inner.inner_ix_index, inner.destination.as_str()), (1, 0, "lookup"));
        assert_eq!((inner.destination_owner.as_deref(), inner.amount), (Some("ownerB"), 1 << 60));
    }

    #[test]
    fn skips_failed_transactions() {
        assert!(decode_transfers(&Tx { failed: true, ..tx() }).is_empty());
    }
}
