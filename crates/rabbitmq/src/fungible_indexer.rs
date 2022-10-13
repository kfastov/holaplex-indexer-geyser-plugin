//! Queue configuration for dispatching documents to be added to a search index.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;

use crate::{
    queue_type::{Binding, QueueProps, RetryProps},
    suffix::Suffix,
    Result,
};

/// Message data for a search index job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Fungible token account update
    FungibleTokenAccountUpdate {
        /// Owner of the token account
        owner: Pubkey,
        /// Mint of the token account
        mint: Pubkey,
        /// Token account address
        address: Pubkey,
        /// Amount of tokens in the account
        amount: u64,
    },
    /// Fungible token mint account update
    FungibleMintAccountUpdate {
        /// Mint address
        mint: Pubkey,
        /// Mint authority
        authority: Option<Pubkey>,
        /// Mint decimals
        decimals: u8,
        /// Mint supply
        supply: u64,
    },
    /// Fungible token MPL metadata account update
    FungibleMetadataUpdate {
        /// Metadata address
        address: Pubkey,
        /// Mint address
        mint: Pubkey,
        /// Metadata name
        name: String,
        /// Metadata symbol
        symbol: String,
        /// Metadata URI
        uri: String,
    },
}

/// AMQP configuration for fungible indexers
#[derive(Debug, Clone)]
pub struct QueueType {
    props: QueueProps,
}

impl QueueType {
    /// Construct a new queue configuration given the expected sender and
    /// queue suffix configuration
    ///
    /// # Errors
    /// This function fails if the given queue suffix is invalid.
    pub fn new(sender: &str, suffix: &Suffix) -> Result<Self> {
        let exchange = format!("{}.fungible", sender);
        let queue = suffix.format(format!("{}.indexer", exchange))?;

        Ok(Self {
            props: QueueProps {
                exchange,
                queue,
                binding: Binding::Fanout,
                prefetch: 4096,
                max_len_bytes: 100 * 1024 * 1024, // 100 MiB
                auto_delete: suffix.is_debug(),
                retry: Some(RetryProps {
                    max_tries: 3,
                    delay_hint: Duration::from_millis(500),
                    max_delay: Duration::from_secs(10 * 60),
                }),
            },
        })
    }
}

impl crate::QueueType for QueueType {
    type Message = Message;

    #[inline]
    fn info(&self) -> crate::queue_type::QueueInfo {
        (&self.props).into()
    }
}

/// The type of a fungible indexer producer
#[cfg(feature = "producer")]
pub type Producer = crate::producer::Producer<QueueType>;
/// The type of a fungible indexer consumer
#[cfg(feature = "consumer")]
pub type Consumer = crate::consumer::Consumer<QueueType>;
