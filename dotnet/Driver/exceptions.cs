using System;
using Grpc.Core;

namespace StreamsDB.Driver
{
        internal static class ExceptionConverter {
        public static (Exception, bool) Convert(Exception ex) {
            switch (ex) {
                case RpcException e:
                    switch(e.Status.StatusCode) {
                        case StatusCode.NotFound:
                            return (new NotFoundException(e.Status.Detail, ex), true);
                        case StatusCode.Aborted:
                            return (new OperationAbortedException(e.Status.Detail, ex), true);
                        case StatusCode.PermissionDenied:
                            return (new PermissionDeniedException(e.Status.Detail, ex), true);
                        default:
                            return (new UnknownStatusException($"status {e.Status.StatusCode} not known: {e.Status.Detail}", ex), true);
                    }
                default:
                    return (ex, false);
            }
        }
    }

    [System.Serializable]
    public class UnknownStatusException : StreamsDBException
    {
        public UnknownStatusException() { }
        public UnknownStatusException(string message) : base(message) { }
        public UnknownStatusException(string message, System.Exception inner) : base(message, inner) { }
        protected UnknownStatusException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// Base exception for all well known StreamsDB exceptions.
    /// </summary>
    [System.Serializable]
    public class StreamsDBException : System.Exception
    {
        public StreamsDBException() { }
        public StreamsDBException(string message) : base(message) { }
        public StreamsDBException(string message, System.Exception inner) : base(message, inner) { }
        protected StreamsDBException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    [System.Serializable]
    public class PermissionDeniedException : StreamsDBException
    {
        public PermissionDeniedException() { }
        public PermissionDeniedException(string message) : base(message) { }
        public PermissionDeniedException(string message, System.Exception inner) : base(message, inner) { }
        protected PermissionDeniedException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// Some requested entity (e.g., database, user or stream) was not found.
    /// </summary>
    [System.Serializable]
    public class NotFoundException : StreamsDBException
    {
        public NotFoundException() { }
        public NotFoundException(string message) : base(message) { }
        public NotFoundException(string message, System.Exception inner) : base(message, inner) { }
        protected NotFoundException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }

    /// <summary>
    /// The operation was aborted, typically due to a concurrency issue such as a concurrency check failure.
    /// </summary>
    [System.Serializable]
    public class OperationAbortedException : StreamsDBException
    {
        public OperationAbortedException() { }
        public OperationAbortedException(string message) : base(message) { }
        public OperationAbortedException(string message, System.Exception inner) : base(message, inner) { }
        protected OperationAbortedException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}