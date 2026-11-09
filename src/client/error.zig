/// Client state-machine protocol, state, and capacity failures.
pub const ClientError = error{
    InvalidPacket,
    InvalidState,
    /// A bounded queue or payload pool is exhausted.
    IoError,
    PayloadTooLarge,
};
