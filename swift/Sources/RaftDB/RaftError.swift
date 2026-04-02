import Foundation

/// Maps the C `RftError` enum (uint32_t) to typed Swift errors.
///
/// Error codes match `core/include/raft.h`:
/// - 0 = OK (no error)
/// - 1 = NullPointer
/// - 2 = InvalidUtf8
/// - 3 = IoError
/// - 4 = NotFound
/// - 5 = BufferTooSmall
public enum RaftError: Error, Equatable, CustomStringConvertible {

    /// A required pointer argument was null (code 1).
    case nullPointer

    /// A string argument was not valid UTF-8 (code 2).
    case invalidUtf8

    /// An I/O or storage engine error occurred (code 3).
    case ioError

    /// The requested key was not found (code 4).
    case notFound

    /// The caller-provided buffer is too small (code 5).
    case bufferTooSmall

    /// An unknown error code was returned.
    case unknown(UInt32)

    // MARK: - Mapping

    /// The raw C error code.
    public var code: UInt32 {
        switch self {
        case .nullPointer:    return 1
        case .invalidUtf8:    return 2
        case .ioError:        return 3
        case .notFound:       return 4
        case .bufferTooSmall: return 5
        case .unknown(let c): return c
        }
    }

    /// Creates a ``RaftError`` from a raw C error code.
    ///
    /// Returns `nil` for code 0 (OK).
    public static func fromCode(_ code: UInt32) -> RaftError? {
        switch code {
        case 0: return nil
        case 1: return .nullPointer
        case 2: return .invalidUtf8
        case 3: return .ioError
        case 4: return .notFound
        case 5: return .bufferTooSmall
        default: return .unknown(code)
        }
    }

    /// Throws if `code` is non-zero.
    static func check(_ code: UInt32) throws {
        if let error = fromCode(code) {
            throw error
        }
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        switch self {
        case .nullPointer:    return "RaftError.nullPointer: A required pointer argument was null"
        case .invalidUtf8:    return "RaftError.invalidUtf8: A string argument was not valid UTF-8"
        case .ioError:        return "RaftError.ioError: An I/O or storage engine error occurred"
        case .notFound:       return "RaftError.notFound: The requested key was not found"
        case .bufferTooSmall: return "RaftError.bufferTooSmall: The caller-provided buffer is too small"
        case .unknown(let c): return "RaftError.unknown: Unknown error code \(c)"
        }
    }
}
