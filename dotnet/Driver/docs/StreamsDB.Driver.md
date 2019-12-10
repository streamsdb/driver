<a name='assembly'></a>
# StreamsDB.Driver

## Contents

- [ApiReflection](#T-StreamsDB-Driver-Wire-ApiReflection 'StreamsDB.Driver.Wire.ApiReflection')
  - [Descriptor](#P-StreamsDB-Driver-Wire-ApiReflection-Descriptor 'StreamsDB.Driver.Wire.ApiReflection.Descriptor')
- [AppendStreamReply](#T-StreamsDB-Driver-Wire-AppendStreamReply 'StreamsDB.Driver.Wire.AppendStreamReply')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamReply-FromFieldNumber 'StreamsDB.Driver.Wire.AppendStreamReply.FromFieldNumber')
  - [VersionFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamReply-VersionFieldNumber 'StreamsDB.Driver.Wire.AppendStreamReply.VersionFieldNumber')
- [AppendStreamRequest](#T-StreamsDB-Driver-Wire-AppendStreamRequest 'StreamsDB.Driver.Wire.AppendStreamRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.AppendStreamRequest.DatabaseFieldNumber')
  - [ExpectedVersionFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamRequest-ExpectedVersionFieldNumber 'StreamsDB.Driver.Wire.AppendStreamRequest.ExpectedVersionFieldNumber')
  - [MessagesFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamRequest-MessagesFieldNumber 'StreamsDB.Driver.Wire.AppendStreamRequest.MessagesFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamRequest-StreamFieldNumber 'StreamsDB.Driver.Wire.AppendStreamRequest.StreamFieldNumber')
- [AppendStreamsReply](#T-StreamsDB-Driver-Wire-AppendStreamsReply 'StreamsDB.Driver.Wire.AppendStreamsReply')
  - [ResultsFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamsReply-ResultsFieldNumber 'StreamsDB.Driver.Wire.AppendStreamsReply.ResultsFieldNumber')
- [AppendStreamsRequest](#T-StreamsDB-Driver-Wire-AppendStreamsRequest 'StreamsDB.Driver.Wire.AppendStreamsRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamsRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.AppendStreamsRequest.DatabaseFieldNumber')
  - [InputsFieldNumber](#F-StreamsDB-Driver-Wire-AppendStreamsRequest-InputsFieldNumber 'StreamsDB.Driver.Wire.AppendStreamsRequest.InputsFieldNumber')
- [ChangePasswordRequest](#T-StreamsDB-Driver-Wire-ChangePasswordRequest 'StreamsDB.Driver.Wire.ChangePasswordRequest')
  - [PasswordFieldNumber](#F-StreamsDB-Driver-Wire-ChangePasswordRequest-PasswordFieldNumber 'StreamsDB.Driver.Wire.ChangePasswordRequest.PasswordFieldNumber')
  - [UsernameFieldNumber](#F-StreamsDB-Driver-Wire-ChangePasswordRequest-UsernameFieldNumber 'StreamsDB.Driver.Wire.ChangePasswordRequest.UsernameFieldNumber')
- [ConcurrencyCheck](#T-StreamsDB-Driver-ConcurrencyCheck 'StreamsDB.Driver.ConcurrencyCheck')
  - [ExpectStreamLastMessage(message)](#M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamLastMessage-StreamsDB-Driver-Message- 'StreamsDB.Driver.ConcurrencyCheck.ExpectStreamLastMessage(StreamsDB.Driver.Message)')
  - [ExpectStreamNotExists()](#M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamNotExists 'StreamsDB.Driver.ConcurrencyCheck.ExpectStreamNotExists')
  - [ExpectStreamVersion(version)](#M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamVersion-System-Int64- 'StreamsDB.Driver.ConcurrencyCheck.ExpectStreamVersion(System.Int64)')
  - [Skip()](#M-StreamsDB-Driver-ConcurrencyCheck-Skip 'StreamsDB.Driver.ConcurrencyCheck.Skip')
- [CreateDatabaseRequest](#T-StreamsDB-Driver-Wire-CreateDatabaseRequest 'StreamsDB.Driver.Wire.CreateDatabaseRequest')
  - [NameFieldNumber](#F-StreamsDB-Driver-Wire-CreateDatabaseRequest-NameFieldNumber 'StreamsDB.Driver.Wire.CreateDatabaseRequest.NameFieldNumber')
- [CreateUserRequest](#T-StreamsDB-Driver-Wire-CreateUserRequest 'StreamsDB.Driver.Wire.CreateUserRequest')
  - [PasswordFieldNumber](#F-StreamsDB-Driver-Wire-CreateUserRequest-PasswordFieldNumber 'StreamsDB.Driver.Wire.CreateUserRequest.PasswordFieldNumber')
  - [UsernameFieldNumber](#F-StreamsDB-Driver-Wire-CreateUserRequest-UsernameFieldNumber 'StreamsDB.Driver.Wire.CreateUserRequest.UsernameFieldNumber')
- [DB](#T-StreamsDB-Driver-DB 'StreamsDB.Driver.DB')
  - [AppendStream(streamId,messages)](#M-StreamsDB-Driver-DB-AppendStream-System-String,System-Collections-Generic-IEnumerable{StreamsDB-Driver-MessageInput}- 'StreamsDB.Driver.DB.AppendStream(System.String,System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput})')
  - [AppendStream(streamId,messages)](#M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-MessageInput[]- 'StreamsDB.Driver.DB.AppendStream(System.String,StreamsDB.Driver.MessageInput[])')
  - [AppendStream(streamId,concurrencyCheck,messages)](#M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-ConcurrencyCheck,StreamsDB-Driver-MessageInput[]- 'StreamsDB.Driver.DB.AppendStream(System.String,StreamsDB.Driver.ConcurrencyCheck,StreamsDB.Driver.MessageInput[])')
  - [AppendStream(streamId,concurrencyCheck,messages)](#M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-ConcurrencyCheck,System-Collections-Generic-IEnumerable{StreamsDB-Driver-MessageInput}- 'StreamsDB.Driver.DB.AppendStream(System.String,StreamsDB.Driver.ConcurrencyCheck,System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput})')
  - [ReadLastMessageFromStream()](#M-StreamsDB-Driver-DB-ReadLastMessageFromStream-System-String- 'StreamsDB.Driver.DB.ReadLastMessageFromStream(System.String)')
  - [ReadMessageFromStream()](#M-StreamsDB-Driver-DB-ReadMessageFromStream-System-String,System-Int64- 'StreamsDB.Driver.DB.ReadMessageFromStream(System.String,System.Int64)')
  - [ReadStreamBackward(streamId,from,limit)](#M-StreamsDB-Driver-DB-ReadStreamBackward-System-String,System-Int64,System-Int32- 'StreamsDB.Driver.DB.ReadStreamBackward(System.String,System.Int64,System.Int32)')
  - [ReadStreamForward(streamId,from,limit)](#M-StreamsDB-Driver-DB-ReadStreamForward-System-String,System-Int64,System-Int32- 'StreamsDB.Driver.DB.ReadStreamForward(System.String,System.Int64,System.Int32)')
  - [SubscribeStream(streamId,from,cancellationToken)](#M-StreamsDB-Driver-DB-SubscribeStream-System-String,System-Int64,System-Threading-CancellationToken- 'StreamsDB.Driver.DB.SubscribeStream(System.String,System.Int64,System.Threading.CancellationToken)')
- [DeleteMessageRequest](#T-StreamsDB-Driver-Wire-DeleteMessageRequest 'StreamsDB.Driver.Wire.DeleteMessageRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-DeleteMessageRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.DeleteMessageRequest.DatabaseFieldNumber')
  - [PositionFieldNumber](#F-StreamsDB-Driver-Wire-DeleteMessageRequest-PositionFieldNumber 'StreamsDB.Driver.Wire.DeleteMessageRequest.PositionFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-DeleteMessageRequest-StreamFieldNumber 'StreamsDB.Driver.Wire.DeleteMessageRequest.StreamFieldNumber')
- [DeleteStreamRequest](#T-StreamsDB-Driver-Wire-DeleteStreamRequest 'StreamsDB.Driver.Wire.DeleteStreamRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-DeleteStreamRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.DeleteStreamRequest.DatabaseFieldNumber')
  - [ExpectedVersionFieldNumber](#F-StreamsDB-Driver-Wire-DeleteStreamRequest-ExpectedVersionFieldNumber 'StreamsDB.Driver.Wire.DeleteStreamRequest.ExpectedVersionFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-DeleteStreamRequest-StreamFieldNumber 'StreamsDB.Driver.Wire.DeleteStreamRequest.StreamFieldNumber')
- [EnableAclRequest](#T-StreamsDB-Driver-Wire-EnableAclRequest 'StreamsDB.Driver.Wire.EnableAclRequest')
  - [PasswordFieldNumber](#F-StreamsDB-Driver-Wire-EnableAclRequest-PasswordFieldNumber 'StreamsDB.Driver.Wire.EnableAclRequest.PasswordFieldNumber')
  - [SignatureFieldNumber](#F-StreamsDB-Driver-Wire-EnableAclRequest-SignatureFieldNumber 'StreamsDB.Driver.Wire.EnableAclRequest.SignatureFieldNumber')
  - [UsernameFieldNumber](#F-StreamsDB-Driver-Wire-EnableAclRequest-UsernameFieldNumber 'StreamsDB.Driver.Wire.EnableAclRequest.UsernameFieldNumber')
- [GetDatabaseReply](#T-StreamsDB-Driver-Wire-GetDatabaseReply 'StreamsDB.Driver.Wire.GetDatabaseReply')
  - [NameFieldNumber](#F-StreamsDB-Driver-Wire-GetDatabaseReply-NameFieldNumber 'StreamsDB.Driver.Wire.GetDatabaseReply.NameFieldNumber')
- [GetDatabaseRequest](#T-StreamsDB-Driver-Wire-GetDatabaseRequest 'StreamsDB.Driver.Wire.GetDatabaseRequest')
  - [NameFieldNumber](#F-StreamsDB-Driver-Wire-GetDatabaseRequest-NameFieldNumber 'StreamsDB.Driver.Wire.GetDatabaseRequest.NameFieldNumber')
- [GetDatabasesReply](#T-StreamsDB-Driver-Wire-GetDatabasesReply 'StreamsDB.Driver.Wire.GetDatabasesReply')
  - [DatabasesFieldNumber](#F-StreamsDB-Driver-Wire-GetDatabasesReply-DatabasesFieldNumber 'StreamsDB.Driver.Wire.GetDatabasesReply.DatabasesFieldNumber')
- [GetStreamsReply](#T-StreamsDB-Driver-Wire-GetStreamsReply 'StreamsDB.Driver.Wire.GetStreamsReply')
  - [AfterCursorFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-AfterCursorFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.AfterCursorFieldNumber')
  - [BeforeCursorFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-BeforeCursorFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.BeforeCursorFieldNumber')
  - [CursorFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-CursorFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.CursorFieldNumber')
  - [DirectionFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-DirectionFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.DirectionFieldNumber')
  - [HasAfterFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-HasAfterFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.HasAfterFieldNumber')
  - [HasBeforeFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-HasBeforeFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.HasBeforeFieldNumber')
  - [ResultFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-ResultFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.ResultFieldNumber')
  - [TotalFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsReply-TotalFieldNumber 'StreamsDB.Driver.Wire.GetStreamsReply.TotalFieldNumber')
- [GetStreamsRequest](#T-StreamsDB-Driver-Wire-GetStreamsRequest 'StreamsDB.Driver.Wire.GetStreamsRequest')
  - [CursorFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsRequest-CursorFieldNumber 'StreamsDB.Driver.Wire.GetStreamsRequest.CursorFieldNumber')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.GetStreamsRequest.DatabaseFieldNumber')
  - [DirectionFieldNumber](#F-StreamsDB-Driver-Wire-GetStreamsRequest-DirectionFieldNumber 'StreamsDB.Driver.Wire.GetStreamsRequest.DirectionFieldNumber')
- [GlobalPosition](#T-StreamsDB-Driver-GlobalPosition 'StreamsDB.Driver.GlobalPosition')
  - [Begin](#F-StreamsDB-Driver-GlobalPosition-Begin 'StreamsDB.Driver.GlobalPosition.Begin')
- [GrandUserToDatabaseRequest](#T-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest 'StreamsDB.Driver.Wire.GrandUserToDatabaseRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.GrandUserToDatabaseRequest.DatabaseFieldNumber')
  - [UsernameFieldNumber](#F-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest-UsernameFieldNumber 'StreamsDB.Driver.Wire.GrandUserToDatabaseRequest.UsernameFieldNumber')
- [IStreamSlice](#T-StreamsDB-Driver-IStreamSlice 'StreamsDB.Driver.IStreamSlice')
  - [From](#P-StreamsDB-Driver-IStreamSlice-From 'StreamsDB.Driver.IStreamSlice.From')
  - [HasNext](#P-StreamsDB-Driver-IStreamSlice-HasNext 'StreamsDB.Driver.IStreamSlice.HasNext')
  - [Messages](#P-StreamsDB-Driver-IStreamSlice-Messages 'StreamsDB.Driver.IStreamSlice.Messages')
  - [Next](#P-StreamsDB-Driver-IStreamSlice-Next 'StreamsDB.Driver.IStreamSlice.Next')
  - [Reverse](#P-StreamsDB-Driver-IStreamSlice-Reverse 'StreamsDB.Driver.IStreamSlice.Reverse')
  - [Stream](#P-StreamsDB-Driver-IStreamSlice-Stream 'StreamsDB.Driver.IStreamSlice.Stream')
- [IStreamSubscription](#T-StreamsDB-Driver-IStreamSubscription 'StreamsDB.Driver.IStreamSubscription')
- [LoginReply](#T-StreamsDB-Driver-Wire-LoginReply 'StreamsDB.Driver.Wire.LoginReply')
  - [TokenFieldNumber](#F-StreamsDB-Driver-Wire-LoginReply-TokenFieldNumber 'StreamsDB.Driver.Wire.LoginReply.TokenFieldNumber')
- [LoginRequest](#T-StreamsDB-Driver-Wire-LoginRequest 'StreamsDB.Driver.Wire.LoginRequest')
  - [PasswordFieldNumber](#F-StreamsDB-Driver-Wire-LoginRequest-PasswordFieldNumber 'StreamsDB.Driver.Wire.LoginRequest.PasswordFieldNumber')
  - [UsernameFieldNumber](#F-StreamsDB-Driver-Wire-LoginRequest-UsernameFieldNumber 'StreamsDB.Driver.Wire.LoginRequest.UsernameFieldNumber')
- [Message](#T-StreamsDB-Driver-Message 'StreamsDB.Driver.Message')
- [Message](#T-StreamsDB-Driver-Wire-Message 'StreamsDB.Driver.Wire.Message')
  - [HeaderFieldNumber](#F-StreamsDB-Driver-Wire-Message-HeaderFieldNumber 'StreamsDB.Driver.Wire.Message.HeaderFieldNumber')
  - [IdFieldNumber](#F-StreamsDB-Driver-Wire-Message-IdFieldNumber 'StreamsDB.Driver.Wire.Message.IdFieldNumber')
  - [PositionFieldNumber](#F-StreamsDB-Driver-Wire-Message-PositionFieldNumber 'StreamsDB.Driver.Wire.Message.PositionFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-Message-StreamFieldNumber 'StreamsDB.Driver.Wire.Message.StreamFieldNumber')
  - [TimestampFieldNumber](#F-StreamsDB-Driver-Wire-Message-TimestampFieldNumber 'StreamsDB.Driver.Wire.Message.TimestampFieldNumber')
  - [TypeFieldNumber](#F-StreamsDB-Driver-Wire-Message-TypeFieldNumber 'StreamsDB.Driver.Wire.Message.TypeFieldNumber')
  - [ValueFieldNumber](#F-StreamsDB-Driver-Wire-Message-ValueFieldNumber 'StreamsDB.Driver.Wire.Message.ValueFieldNumber')
  - [Header](#P-StreamsDB-Driver-Message-Header 'StreamsDB.Driver.Message.Header')
  - [Position](#P-StreamsDB-Driver-Message-Position 'StreamsDB.Driver.Message.Position')
  - [Stream](#P-StreamsDB-Driver-Message-Stream 'StreamsDB.Driver.Message.Stream')
  - [Timestamp](#P-StreamsDB-Driver-Message-Timestamp 'StreamsDB.Driver.Message.Timestamp')
  - [Type](#P-StreamsDB-Driver-Message-Type 'StreamsDB.Driver.Message.Type')
  - [Value](#P-StreamsDB-Driver-Message-Value 'StreamsDB.Driver.Message.Value')
- [MessageInput](#T-StreamsDB-Driver-MessageInput 'StreamsDB.Driver.MessageInput')
- [MessageInput](#T-StreamsDB-Driver-Wire-MessageInput 'StreamsDB.Driver.Wire.MessageInput')
  - [HeaderFieldNumber](#F-StreamsDB-Driver-Wire-MessageInput-HeaderFieldNumber 'StreamsDB.Driver.Wire.MessageInput.HeaderFieldNumber')
  - [IdFieldNumber](#F-StreamsDB-Driver-Wire-MessageInput-IdFieldNumber 'StreamsDB.Driver.Wire.MessageInput.IdFieldNumber')
  - [TypeFieldNumber](#F-StreamsDB-Driver-Wire-MessageInput-TypeFieldNumber 'StreamsDB.Driver.Wire.MessageInput.TypeFieldNumber')
  - [ValueFieldNumber](#F-StreamsDB-Driver-Wire-MessageInput-ValueFieldNumber 'StreamsDB.Driver.Wire.MessageInput.ValueFieldNumber')
  - [Header](#P-StreamsDB-Driver-MessageInput-Header 'StreamsDB.Driver.MessageInput.Header')
  - [ID](#P-StreamsDB-Driver-MessageInput-ID 'StreamsDB.Driver.MessageInput.ID')
  - [Type](#P-StreamsDB-Driver-MessageInput-Type 'StreamsDB.Driver.MessageInput.Type')
  - [Value](#P-StreamsDB-Driver-MessageInput-Value 'StreamsDB.Driver.MessageInput.Value')
- [NotFoundException](#T-StreamsDB-Driver-NotFoundException 'StreamsDB.Driver.NotFoundException')
- [OperationAbortedException](#T-StreamsDB-Driver-OperationAbortedException 'StreamsDB.Driver.OperationAbortedException')
- [ReadGlobalReply](#T-StreamsDB-Driver-Wire-ReadGlobalReply 'StreamsDB.Driver.Wire.ReadGlobalReply')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalReply-DatabaseFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalReply.DatabaseFieldNumber')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalReply-FromFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalReply.FromFieldNumber')
  - [HasNextFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalReply-HasNextFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalReply.HasNextFieldNumber')
  - [MessagesFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalReply-MessagesFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalReply.MessagesFieldNumber')
  - [NextFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalReply-NextFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalReply.NextFieldNumber')
- [ReadGlobalRequest](#T-StreamsDB-Driver-Wire-ReadGlobalRequest 'StreamsDB.Driver.Wire.ReadGlobalRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalRequest.DatabaseFieldNumber')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalRequest-FromFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalRequest.FromFieldNumber')
  - [LimitFieldNumber](#F-StreamsDB-Driver-Wire-ReadGlobalRequest-LimitFieldNumber 'StreamsDB.Driver.Wire.ReadGlobalRequest.LimitFieldNumber')
- [ReadStreamRequest](#T-StreamsDB-Driver-Wire-ReadStreamRequest 'StreamsDB.Driver.Wire.ReadStreamRequest')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-ReadStreamRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.ReadStreamRequest.DatabaseFieldNumber')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-ReadStreamRequest-FromFieldNumber 'StreamsDB.Driver.Wire.ReadStreamRequest.FromFieldNumber')
  - [LimitFieldNumber](#F-StreamsDB-Driver-Wire-ReadStreamRequest-LimitFieldNumber 'StreamsDB.Driver.Wire.ReadStreamRequest.LimitFieldNumber')
  - [ReverseFieldNumber](#F-StreamsDB-Driver-Wire-ReadStreamRequest-ReverseFieldNumber 'StreamsDB.Driver.Wire.ReadStreamRequest.ReverseFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-ReadStreamRequest-StreamFieldNumber 'StreamsDB.Driver.Wire.ReadStreamRequest.StreamFieldNumber')
- [Slice](#T-StreamsDB-Driver-Wire-Slice 'StreamsDB.Driver.Wire.Slice')
  - [CountFieldNumber](#F-StreamsDB-Driver-Wire-Slice-CountFieldNumber 'StreamsDB.Driver.Wire.Slice.CountFieldNumber')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-Slice-FromFieldNumber 'StreamsDB.Driver.Wire.Slice.FromFieldNumber')
  - [HasNextFieldNumber](#F-StreamsDB-Driver-Wire-Slice-HasNextFieldNumber 'StreamsDB.Driver.Wire.Slice.HasNextFieldNumber')
  - [HeadFieldNumber](#F-StreamsDB-Driver-Wire-Slice-HeadFieldNumber 'StreamsDB.Driver.Wire.Slice.HeadFieldNumber')
  - [MessagesFieldNumber](#F-StreamsDB-Driver-Wire-Slice-MessagesFieldNumber 'StreamsDB.Driver.Wire.Slice.MessagesFieldNumber')
  - [NextFieldNumber](#F-StreamsDB-Driver-Wire-Slice-NextFieldNumber 'StreamsDB.Driver.Wire.Slice.NextFieldNumber')
  - [ReverseFieldNumber](#F-StreamsDB-Driver-Wire-Slice-ReverseFieldNumber 'StreamsDB.Driver.Wire.Slice.ReverseFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-Slice-StreamFieldNumber 'StreamsDB.Driver.Wire.Slice.StreamFieldNumber')
- [StreamAppendResult](#T-StreamsDB-Driver-Wire-StreamAppendResult 'StreamsDB.Driver.Wire.StreamAppendResult')
  - [PositionFieldNumber](#F-StreamsDB-Driver-Wire-StreamAppendResult-PositionFieldNumber 'StreamsDB.Driver.Wire.StreamAppendResult.PositionFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-StreamAppendResult-StreamFieldNumber 'StreamsDB.Driver.Wire.StreamAppendResult.StreamFieldNumber')
  - [VersionFieldNumber](#F-StreamsDB-Driver-Wire-StreamAppendResult-VersionFieldNumber 'StreamsDB.Driver.Wire.StreamAppendResult.VersionFieldNumber')
- [StreamInput](#T-StreamsDB-Driver-StreamInput 'StreamsDB.Driver.StreamInput')
- [StreamInput](#T-StreamsDB-Driver-Wire-StreamInput 'StreamsDB.Driver.Wire.StreamInput')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-StreamInput-DatabaseFieldNumber 'StreamsDB.Driver.Wire.StreamInput.DatabaseFieldNumber')
  - [ExpectedVersionFieldNumber](#F-StreamsDB-Driver-Wire-StreamInput-ExpectedVersionFieldNumber 'StreamsDB.Driver.Wire.StreamInput.ExpectedVersionFieldNumber')
  - [MessagesFieldNumber](#F-StreamsDB-Driver-Wire-StreamInput-MessagesFieldNumber 'StreamsDB.Driver.Wire.StreamInput.MessagesFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-StreamInput-StreamFieldNumber 'StreamsDB.Driver.Wire.StreamInput.StreamFieldNumber')
- [Streams](#T-StreamsDB-Driver-Wire-Streams 'StreamsDB.Driver.Wire.Streams')
  - [Descriptor](#P-StreamsDB-Driver-Wire-Streams-Descriptor 'StreamsDB.Driver.Wire.Streams.Descriptor')
- [StreamsClient](#T-StreamsDB-Driver-Wire-Streams-StreamsClient 'StreamsDB.Driver.Wire.Streams.StreamsClient')
  - [#ctor(channel)](#M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-Channel- 'StreamsDB.Driver.Wire.Streams.StreamsClient.#ctor(Grpc.Core.Channel)')
  - [#ctor(callInvoker)](#M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-CallInvoker- 'StreamsDB.Driver.Wire.Streams.StreamsClient.#ctor(Grpc.Core.CallInvoker)')
  - [#ctor()](#M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor 'StreamsDB.Driver.Wire.Streams.StreamsClient.#ctor')
  - [#ctor(configuration)](#M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-ClientBase-ClientBaseConfiguration- 'StreamsDB.Driver.Wire.Streams.StreamsClient.#ctor(Grpc.Core.ClientBase.ClientBaseConfiguration)')
  - [NewInstance()](#M-StreamsDB-Driver-Wire-Streams-StreamsClient-NewInstance-Grpc-Core-ClientBase-ClientBaseConfiguration- 'StreamsDB.Driver.Wire.Streams.StreamsClient.NewInstance(Grpc.Core.ClientBase.ClientBaseConfiguration)')
- [StreamsDBClient](#T-StreamsDB-Driver-StreamsDBClient 'StreamsDB.Driver.StreamsDBClient')
  - [Connect(connectionString)](#M-StreamsDB-Driver-StreamsDBClient-Connect-System-String- 'StreamsDB.Driver.StreamsDBClient.Connect(System.String)')
  - [DB(db)](#M-StreamsDB-Driver-StreamsDBClient-DB-System-String- 'StreamsDB.Driver.StreamsDBClient.DB(System.String)')
- [StreamsDBException](#T-StreamsDB-Driver-StreamsDBException 'StreamsDB.Driver.StreamsDBException')
- [SubscribeStreamRequest](#T-StreamsDB-Driver-Wire-SubscribeStreamRequest 'StreamsDB.Driver.Wire.SubscribeStreamRequest')
  - [CountFieldNumber](#F-StreamsDB-Driver-Wire-SubscribeStreamRequest-CountFieldNumber 'StreamsDB.Driver.Wire.SubscribeStreamRequest.CountFieldNumber')
  - [DatabaseFieldNumber](#F-StreamsDB-Driver-Wire-SubscribeStreamRequest-DatabaseFieldNumber 'StreamsDB.Driver.Wire.SubscribeStreamRequest.DatabaseFieldNumber')
  - [FromFieldNumber](#F-StreamsDB-Driver-Wire-SubscribeStreamRequest-FromFieldNumber 'StreamsDB.Driver.Wire.SubscribeStreamRequest.FromFieldNumber')
  - [StreamFieldNumber](#F-StreamsDB-Driver-Wire-SubscribeStreamRequest-StreamFieldNumber 'StreamsDB.Driver.Wire.SubscribeStreamRequest.StreamFieldNumber')

<a name='T-StreamsDB-Driver-Wire-ApiReflection'></a>
## ApiReflection `type`

##### Namespace

StreamsDB.Driver.Wire

##### Summary

Holder for reflection information generated from Wire/api.proto

<a name='P-StreamsDB-Driver-Wire-ApiReflection-Descriptor'></a>
### Descriptor `property`

##### Summary

File descriptor for Wire/api.proto

<a name='T-StreamsDB-Driver-Wire-AppendStreamReply'></a>
## AppendStreamReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-AppendStreamReply-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-AppendStreamReply-VersionFieldNumber'></a>
### VersionFieldNumber `constants`

##### Summary

Field number for the "version" field.

<a name='T-StreamsDB-Driver-Wire-AppendStreamRequest'></a>
## AppendStreamRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-AppendStreamRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-AppendStreamRequest-ExpectedVersionFieldNumber'></a>
### ExpectedVersionFieldNumber `constants`

##### Summary

Field number for the "expectedVersion" field.

<a name='F-StreamsDB-Driver-Wire-AppendStreamRequest-MessagesFieldNumber'></a>
### MessagesFieldNumber `constants`

##### Summary

Field number for the "messages" field.

<a name='F-StreamsDB-Driver-Wire-AppendStreamRequest-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-AppendStreamsReply'></a>
## AppendStreamsReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-AppendStreamsReply-ResultsFieldNumber'></a>
### ResultsFieldNumber `constants`

##### Summary

Field number for the "results" field.

<a name='T-StreamsDB-Driver-Wire-AppendStreamsRequest'></a>
## AppendStreamsRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-AppendStreamsRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-AppendStreamsRequest-InputsFieldNumber'></a>
### InputsFieldNumber `constants`

##### Summary

Field number for the "inputs" field.

<a name='T-StreamsDB-Driver-Wire-ChangePasswordRequest'></a>
## ChangePasswordRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-ChangePasswordRequest-PasswordFieldNumber'></a>
### PasswordFieldNumber `constants`

##### Summary

Field number for the "password" field.

<a name='F-StreamsDB-Driver-Wire-ChangePasswordRequest-UsernameFieldNumber'></a>
### UsernameFieldNumber `constants`

##### Summary

Field number for the "username" field.

<a name='T-StreamsDB-Driver-ConcurrencyCheck'></a>
## ConcurrencyCheck `type`

##### Namespace

StreamsDB.Driver

##### Summary

ConcurrencyCheck specifies the optimistic concurrency check that needs to succeed before
a write transaction gets committed. If the check reveals conflicting modifications, the
write transaction is aborted.

Use [Skip](#M-StreamsDB-Driver-ConcurrencyCheck-Skip 'StreamsDB.Driver.ConcurrencyCheck.Skip') to disable the optimistic concurrency check, or use one of the other
static methods of this class to specify a check.

##### Example

Here is an example that writes a strict monotonically increasing number to a stream. 
Because of the [ExpectStreamLastMessage](#M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamLastMessage-StreamsDB-Driver-Message- 'StreamsDB.Driver.ConcurrencyCheck.ExpectStreamLastMessage(StreamsDB.Driver.Message)') this 
example could be run concurrently and the numbers on the steam will still be monotonicly increasing:

```
int nextNumber;
ConcurrencyCheck check;
while (true) {
  // read the last message from the stream
  var (message, found) = await db.ReadMessageFromStream("exact-sequence", -1);
  if (found)
  {
    // get the number from the value of the last message and increase
    nextNumber = BitConverter.ToInt32(message.Value) + 1;
    // expect the message we read to be the last message on the stream
    check = ConcurrencyCheck.ExpectLastMessage(message);
  }
  else
  {
    nextNumber = 0;
    check = ConcurrencyCheck.ExpectVersion(0);
  }
  try {
    await db.AppendStream("exact-sequence", check, new MessageInput
    {
      Type = "int32",
      Value = BitConverter.GetBytes(nextNumber)
    });
  } catch(OperationAbortedException caught) {
    // The operation was aborted, typically due to
    // a concurrency issue such as a concurrency check failure.
    continue;
  }
}
```

<a name='M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamLastMessage-StreamsDB-Driver-Message-'></a>
### ExpectStreamLastMessage(message) `method`

##### Summary

Expect the stream to have the version that is equal to the position of the specified messaage.

##### Returns

An optimistic concurrency control check that will only succeed if the stream version is equal to the position of the specified message.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| message | [StreamsDB.Driver.Message](#T-StreamsDB-Driver-Message 'StreamsDB.Driver.Message') | The message to use in the check. |

##### Remarks

It's important to understand that only the position will be verfied, not the actual value of the message nor it's ID or headers.

<a name='M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamNotExists'></a>
### ExpectStreamNotExists() `method`

##### Summary

ExpectStreamNotExists returns an optimistic concurrency check that will only succeed if the stream does not
exist.

##### Returns

An optimistic concurrency control check that will only succeed if the stream does not exist.

##### Parameters

This method has no parameters.

<a name='M-StreamsDB-Driver-ConcurrencyCheck-ExpectStreamVersion-System-Int64-'></a>
### ExpectStreamVersion(version) `method`

##### Summary

Expect the stream to have the specified version.

##### Returns

An optimistic concurrency control check that will only succeed if the stream has the specified version.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| version | [System.Int64](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int64 'System.Int64') | The expected version of the stream. |

<a name='M-StreamsDB-Driver-ConcurrencyCheck-Skip'></a>
### Skip() `method`

##### Summary

Skip returns an optimistic concurrency check that will always succeed. In other words, the optimistic
concurrency control will be disabled for the write transaction where this check is used.

##### Returns

An optimistic concurrency control check that will always succeed.

##### Parameters

This method has no parameters.

<a name='T-StreamsDB-Driver-Wire-CreateDatabaseRequest'></a>
## CreateDatabaseRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-CreateDatabaseRequest-NameFieldNumber'></a>
### NameFieldNumber `constants`

##### Summary

Field number for the "name" field.

<a name='T-StreamsDB-Driver-Wire-CreateUserRequest'></a>
## CreateUserRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-CreateUserRequest-PasswordFieldNumber'></a>
### PasswordFieldNumber `constants`

##### Summary

Field number for the "password" field.

<a name='F-StreamsDB-Driver-Wire-CreateUserRequest-UsernameFieldNumber'></a>
### UsernameFieldNumber `constants`

##### Summary

Field number for the "username" field.

<a name='T-StreamsDB-Driver-DB'></a>
## DB `type`

##### Namespace

StreamsDB.Driver

##### Summary

DB represents a database in StreamsDB.

<a name='M-StreamsDB-Driver-DB-AppendStream-System-String,System-Collections-Generic-IEnumerable{StreamsDB-Driver-MessageInput}-'></a>
### AppendStream(streamId,messages) `method`

##### Summary

AppendStream appends the provides messages to the specified stream.

##### Returns

The position in the stream of the first message that has been written.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to append to. If the stream does not exists, it will be created. |
| messages | [System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput}](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Collections.Generic.IEnumerable 'System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput}') | The messages to append. |

<a name='M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-MessageInput[]-'></a>
### AppendStream(streamId,messages) `method`

##### Summary

AppendStream appends the provides messages to the specified stream.

##### Returns

The position in the stream of the first message that has been written.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to append to. If the stream does not exists, it will be created. |
| messages | [StreamsDB.Driver.MessageInput[]](#T-StreamsDB-Driver-MessageInput[] 'StreamsDB.Driver.MessageInput[]') | The messages to append. |

<a name='M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-ConcurrencyCheck,StreamsDB-Driver-MessageInput[]-'></a>
### AppendStream(streamId,concurrencyCheck,messages) `method`

##### Summary

AppendStream appends the provides messages to the specified stream.

##### Returns

The position in the stream of the first message that has been written.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to append to. If the stream does not exists, it will be created. |
| concurrencyCheck | [StreamsDB.Driver.ConcurrencyCheck](#T-StreamsDB-Driver-ConcurrencyCheck 'StreamsDB.Driver.ConcurrencyCheck') | The optimistic concurrency check. See [ConcurrencyCheck](#T-StreamsDB-Driver-ConcurrencyCheck 'StreamsDB.Driver.ConcurrencyCheck') for different options. |
| messages | [StreamsDB.Driver.MessageInput[]](#T-StreamsDB-Driver-MessageInput[] 'StreamsDB.Driver.MessageInput[]') | The messages to append. |

<a name='M-StreamsDB-Driver-DB-AppendStream-System-String,StreamsDB-Driver-ConcurrencyCheck,System-Collections-Generic-IEnumerable{StreamsDB-Driver-MessageInput}-'></a>
### AppendStream(streamId,concurrencyCheck,messages) `method`

##### Summary

AppendStream appends the provides messages to the specified stream.

##### Returns

The position in the stream of the first message that has been written.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to append to. If the stream does not exists, it will be created. |
| concurrencyCheck | [StreamsDB.Driver.ConcurrencyCheck](#T-StreamsDB-Driver-ConcurrencyCheck 'StreamsDB.Driver.ConcurrencyCheck') | The optimistic concurrency check. See [ConcurrencyCheck](#T-StreamsDB-Driver-ConcurrencyCheck 'StreamsDB.Driver.ConcurrencyCheck') for different options. |
| messages | [System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput}](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Collections.Generic.IEnumerable 'System.Collections.Generic.IEnumerable{StreamsDB.Driver.MessageInput}') | The messages to append. |

<a name='M-StreamsDB-Driver-DB-ReadLastMessageFromStream-System-String-'></a>
### ReadLastMessageFromStream() `method`

##### Summary

ReadLastMessageFromStream returns the last message of a stream.

##### Returns

A tuple containing the message and a boolean indication whether the message was found or not.

##### Parameters

This method has no parameters.

<a name='M-StreamsDB-Driver-DB-ReadMessageFromStream-System-String,System-Int64-'></a>
### ReadMessageFromStream() `method`

##### Summary

ReadMessageFromStream returns the message from the stream at the specified position.

##### Returns

A tuple containing the message and a boolean indication whether the message was found or not.

##### Parameters

This method has no parameters.

<a name='M-StreamsDB-Driver-DB-ReadStreamBackward-System-String,System-Int64,System-Int32-'></a>
### ReadStreamBackward(streamId,from,limit) `method`

##### Summary

ReadStreamBackward reads from a stream in the backward direction, in other words, reading from newer to older messages in the stream.

##### Returns

A stream slice.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to read from. |
| from | [System.Int64](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int64 'System.Int64') | The position to read from. |
| limit | [System.Int32](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int32 'System.Int32') | The maximum number of messages to read. |

<a name='M-StreamsDB-Driver-DB-ReadStreamForward-System-String,System-Int64,System-Int32-'></a>
### ReadStreamForward(streamId,from,limit) `method`

##### Summary

ReadStreamForward reads from a stream in the forward direction, in other words, reading from older to newer messages in a stream.

##### Returns

A stream slice.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to read from. |
| from | [System.Int64](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int64 'System.Int64') | The position to read from. |
| limit | [System.Int32](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int32 'System.Int32') | The maximum number of messages to read. |

<a name='M-StreamsDB-Driver-DB-SubscribeStream-System-String,System-Int64,System-Threading-CancellationToken-'></a>
### SubscribeStream(streamId,from,cancellationToken) `method`

##### Summary

SubscribeStream creates a stream subscription that allows you to read from a stream and receive future writes.

##### Returns

A stream subscription.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| streamId | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The stream to subscribe to. |
| from | [System.Int64](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Int64 'System.Int64') | The position to subscribe from. |
| cancellationToken | [System.Threading.CancellationToken](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.Threading.CancellationToken 'System.Threading.CancellationToken') | The cancellation token to cancel the subscription. |

<a name='T-StreamsDB-Driver-Wire-DeleteMessageRequest'></a>
## DeleteMessageRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-DeleteMessageRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-DeleteMessageRequest-PositionFieldNumber'></a>
### PositionFieldNumber `constants`

##### Summary

Field number for the "position" field.

<a name='F-StreamsDB-Driver-Wire-DeleteMessageRequest-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-DeleteStreamRequest'></a>
## DeleteStreamRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-DeleteStreamRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-DeleteStreamRequest-ExpectedVersionFieldNumber'></a>
### ExpectedVersionFieldNumber `constants`

##### Summary

Field number for the "expectedVersion" field.

<a name='F-StreamsDB-Driver-Wire-DeleteStreamRequest-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-EnableAclRequest'></a>
## EnableAclRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-EnableAclRequest-PasswordFieldNumber'></a>
### PasswordFieldNumber `constants`

##### Summary

Field number for the "password" field.

<a name='F-StreamsDB-Driver-Wire-EnableAclRequest-SignatureFieldNumber'></a>
### SignatureFieldNumber `constants`

##### Summary

Field number for the "signature" field.

<a name='F-StreamsDB-Driver-Wire-EnableAclRequest-UsernameFieldNumber'></a>
### UsernameFieldNumber `constants`

##### Summary

Field number for the "username" field.

<a name='T-StreamsDB-Driver-Wire-GetDatabaseReply'></a>
## GetDatabaseReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GetDatabaseReply-NameFieldNumber'></a>
### NameFieldNumber `constants`

##### Summary

Field number for the "name" field.

<a name='T-StreamsDB-Driver-Wire-GetDatabaseRequest'></a>
## GetDatabaseRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GetDatabaseRequest-NameFieldNumber'></a>
### NameFieldNumber `constants`

##### Summary

Field number for the "name" field.

<a name='T-StreamsDB-Driver-Wire-GetDatabasesReply'></a>
## GetDatabasesReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GetDatabasesReply-DatabasesFieldNumber'></a>
### DatabasesFieldNumber `constants`

##### Summary

Field number for the "databases" field.

<a name='T-StreamsDB-Driver-Wire-GetStreamsReply'></a>
## GetStreamsReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-AfterCursorFieldNumber'></a>
### AfterCursorFieldNumber `constants`

##### Summary

Field number for the "afterCursor" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-BeforeCursorFieldNumber'></a>
### BeforeCursorFieldNumber `constants`

##### Summary

Field number for the "beforeCursor" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-CursorFieldNumber'></a>
### CursorFieldNumber `constants`

##### Summary

Field number for the "cursor" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-DirectionFieldNumber'></a>
### DirectionFieldNumber `constants`

##### Summary

Field number for the "direction" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-HasAfterFieldNumber'></a>
### HasAfterFieldNumber `constants`

##### Summary

Field number for the "hasAfter" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-HasBeforeFieldNumber'></a>
### HasBeforeFieldNumber `constants`

##### Summary

Field number for the "hasBefore" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-ResultFieldNumber'></a>
### ResultFieldNumber `constants`

##### Summary

Field number for the "result" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsReply-TotalFieldNumber'></a>
### TotalFieldNumber `constants`

##### Summary

Field number for the "total" field.

<a name='T-StreamsDB-Driver-Wire-GetStreamsRequest'></a>
## GetStreamsRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GetStreamsRequest-CursorFieldNumber'></a>
### CursorFieldNumber `constants`

##### Summary

Field number for the "cursor" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-GetStreamsRequest-DirectionFieldNumber'></a>
### DirectionFieldNumber `constants`

##### Summary

Field number for the "direction" field.

<a name='T-StreamsDB-Driver-GlobalPosition'></a>
## GlobalPosition `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents a position on the global stream.

<a name='F-StreamsDB-Driver-GlobalPosition-Begin'></a>
### Begin `constants`

##### Summary

Represents the first position of the global stream.

<a name='T-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest'></a>
## GrandUserToDatabaseRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-GrandUserToDatabaseRequest-UsernameFieldNumber'></a>
### UsernameFieldNumber `constants`

##### Summary

Field number for the "username" field.

<a name='T-StreamsDB-Driver-IStreamSlice'></a>
## IStreamSlice `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents a slice of a stream that hold messages from a certain position in a stream.

<a name='P-StreamsDB-Driver-IStreamSlice-From'></a>
### From `property`

##### Summary

Gets the from position of this slice.

<a name='P-StreamsDB-Driver-IStreamSlice-HasNext'></a>
### HasNext `property`

##### Summary

Gets an indication if there are more messages availble at the time of reading.

<a name='P-StreamsDB-Driver-IStreamSlice-Messages'></a>
### Messages `property`

##### Summary

Gets the messages.

<a name='P-StreamsDB-Driver-IStreamSlice-Next'></a>
### Next `property`

##### Summary

Gets the next position to continue reading.

<a name='P-StreamsDB-Driver-IStreamSlice-Reverse'></a>
### Reverse `property`

##### Summary

Gets an indication whether this slice is in reverse direction.

<a name='P-StreamsDB-Driver-IStreamSlice-Stream'></a>
### Stream `property`

##### Summary

Gets the name of the stream.

<a name='T-StreamsDB-Driver-IStreamSubscription'></a>
## IStreamSubscription `type`

##### Namespace

StreamsDB.Driver

##### Summary

IStreamSubscription represents a subscription to a stream that can be used
to get current and future messages from a stream.

<a name='T-StreamsDB-Driver-Wire-LoginReply'></a>
## LoginReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-LoginReply-TokenFieldNumber'></a>
### TokenFieldNumber `constants`

##### Summary

Field number for the "token" field.

<a name='T-StreamsDB-Driver-Wire-LoginRequest'></a>
## LoginRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-LoginRequest-PasswordFieldNumber'></a>
### PasswordFieldNumber `constants`

##### Summary

Field number for the "password" field.

<a name='F-StreamsDB-Driver-Wire-LoginRequest-UsernameFieldNumber'></a>
### UsernameFieldNumber `constants`

##### Summary

Field number for the "username" field.

<a name='T-StreamsDB-Driver-Message'></a>
## Message `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents a message on a stream.

<a name='T-StreamsDB-Driver-Wire-Message'></a>
## Message `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-Message-HeaderFieldNumber'></a>
### HeaderFieldNumber `constants`

##### Summary

Field number for the "header" field.

<a name='F-StreamsDB-Driver-Wire-Message-IdFieldNumber'></a>
### IdFieldNumber `constants`

##### Summary

Field number for the "id" field.

<a name='F-StreamsDB-Driver-Wire-Message-PositionFieldNumber'></a>
### PositionFieldNumber `constants`

##### Summary

Field number for the "position" field.

<a name='F-StreamsDB-Driver-Wire-Message-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='F-StreamsDB-Driver-Wire-Message-TimestampFieldNumber'></a>
### TimestampFieldNumber `constants`

##### Summary

Field number for the "timestamp" field.

<a name='F-StreamsDB-Driver-Wire-Message-TypeFieldNumber'></a>
### TypeFieldNumber `constants`

##### Summary

Field number for the "type" field.

<a name='F-StreamsDB-Driver-Wire-Message-ValueFieldNumber'></a>
### ValueFieldNumber `constants`

##### Summary

Field number for the "value" field.

<a name='P-StreamsDB-Driver-Message-Header'></a>
### Header `property`

##### Summary

The header of this message in bytes.

<a name='P-StreamsDB-Driver-Message-Position'></a>
### Position `property`

##### Summary

The position of this message in the stream.

<a name='P-StreamsDB-Driver-Message-Stream'></a>
### Stream `property`

##### Summary

The name of the stream containing this message.

<a name='P-StreamsDB-Driver-Message-Timestamp'></a>
### Timestamp `property`

##### Summary

The point in time this message was created.

<a name='P-StreamsDB-Driver-Message-Type'></a>
### Type `property`

##### Summary

The type of this message.

<a name='P-StreamsDB-Driver-Message-Value'></a>
### Value `property`

##### Summary

The value of this message in bytes.

<a name='T-StreamsDB-Driver-MessageInput'></a>
## MessageInput `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents a message that can be written to a stream.

<a name='T-StreamsDB-Driver-Wire-MessageInput'></a>
## MessageInput `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-MessageInput-HeaderFieldNumber'></a>
### HeaderFieldNumber `constants`

##### Summary

Field number for the "header" field.

<a name='F-StreamsDB-Driver-Wire-MessageInput-IdFieldNumber'></a>
### IdFieldNumber `constants`

##### Summary

Field number for the "id" field.

<a name='F-StreamsDB-Driver-Wire-MessageInput-TypeFieldNumber'></a>
### TypeFieldNumber `constants`

##### Summary

Field number for the "type" field.

<a name='F-StreamsDB-Driver-Wire-MessageInput-ValueFieldNumber'></a>
### ValueFieldNumber `constants`

##### Summary

Field number for the "value" field.

<a name='P-StreamsDB-Driver-MessageInput-Header'></a>
### Header `property`

##### Summary

Gets or sets the header of the message.

<a name='P-StreamsDB-Driver-MessageInput-ID'></a>
### ID `property`

##### Summary

Gets or sets the ID of the message.

<a name='P-StreamsDB-Driver-MessageInput-Type'></a>
### Type `property`

##### Summary

Gets or sets the type of the message.

<a name='P-StreamsDB-Driver-MessageInput-Value'></a>
### Value `property`

##### Summary

Gets or sets the value of the message.

<a name='T-StreamsDB-Driver-NotFoundException'></a>
## NotFoundException `type`

##### Namespace

StreamsDB.Driver

##### Summary

Some requested entity (e.g., database, user or stream) was not found.

<a name='T-StreamsDB-Driver-OperationAbortedException'></a>
## OperationAbortedException `type`

##### Namespace

StreamsDB.Driver

##### Summary

The operation was aborted, typically due to a concurrency issue such as a concurrency check failure.

<a name='T-StreamsDB-Driver-Wire-ReadGlobalReply'></a>
## ReadGlobalReply `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-ReadGlobalReply-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalReply-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalReply-HasNextFieldNumber'></a>
### HasNextFieldNumber `constants`

##### Summary

Field number for the "hasNext" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalReply-MessagesFieldNumber'></a>
### MessagesFieldNumber `constants`

##### Summary

Field number for the "messages" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalReply-NextFieldNumber'></a>
### NextFieldNumber `constants`

##### Summary

Field number for the "next" field.

<a name='T-StreamsDB-Driver-Wire-ReadGlobalRequest'></a>
## ReadGlobalRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-ReadGlobalRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalRequest-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-ReadGlobalRequest-LimitFieldNumber'></a>
### LimitFieldNumber `constants`

##### Summary

Field number for the "limit" field.

<a name='T-StreamsDB-Driver-Wire-ReadStreamRequest'></a>
## ReadStreamRequest `type`

##### Namespace

StreamsDB.Driver.Wire

##### Summary

The request message containing the user's name.

<a name='F-StreamsDB-Driver-Wire-ReadStreamRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-ReadStreamRequest-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-ReadStreamRequest-LimitFieldNumber'></a>
### LimitFieldNumber `constants`

##### Summary

Field number for the "limit" field.

<a name='F-StreamsDB-Driver-Wire-ReadStreamRequest-ReverseFieldNumber'></a>
### ReverseFieldNumber `constants`

##### Summary

Field number for the "reverse" field.

<a name='F-StreamsDB-Driver-Wire-ReadStreamRequest-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-Slice'></a>
## Slice `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-Slice-CountFieldNumber'></a>
### CountFieldNumber `constants`

##### Summary

Field number for the "count" field.

<a name='F-StreamsDB-Driver-Wire-Slice-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-Slice-HasNextFieldNumber'></a>
### HasNextFieldNumber `constants`

##### Summary

Field number for the "hasNext" field.

<a name='F-StreamsDB-Driver-Wire-Slice-HeadFieldNumber'></a>
### HeadFieldNumber `constants`

##### Summary

Field number for the "head" field.

<a name='F-StreamsDB-Driver-Wire-Slice-MessagesFieldNumber'></a>
### MessagesFieldNumber `constants`

##### Summary

Field number for the "messages" field.

<a name='F-StreamsDB-Driver-Wire-Slice-NextFieldNumber'></a>
### NextFieldNumber `constants`

##### Summary

Field number for the "next" field.

<a name='F-StreamsDB-Driver-Wire-Slice-ReverseFieldNumber'></a>
### ReverseFieldNumber `constants`

##### Summary

Field number for the "reverse" field.

<a name='F-StreamsDB-Driver-Wire-Slice-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-StreamAppendResult'></a>
## StreamAppendResult `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-StreamAppendResult-PositionFieldNumber'></a>
### PositionFieldNumber `constants`

##### Summary

Field number for the "position" field.

<a name='F-StreamsDB-Driver-Wire-StreamAppendResult-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='F-StreamsDB-Driver-Wire-StreamAppendResult-VersionFieldNumber'></a>
### VersionFieldNumber `constants`

##### Summary

Field number for the "version" field.

<a name='T-StreamsDB-Driver-StreamInput'></a>
## StreamInput `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents input for a atomic stream write.

<a name='T-StreamsDB-Driver-Wire-StreamInput'></a>
## StreamInput `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-StreamInput-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-StreamInput-ExpectedVersionFieldNumber'></a>
### ExpectedVersionFieldNumber `constants`

##### Summary

Field number for the "expectedVersion" field.

<a name='F-StreamsDB-Driver-Wire-StreamInput-MessagesFieldNumber'></a>
### MessagesFieldNumber `constants`

##### Summary

Field number for the "messages" field.

<a name='F-StreamsDB-Driver-Wire-StreamInput-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.

<a name='T-StreamsDB-Driver-Wire-Streams'></a>
## Streams `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='P-StreamsDB-Driver-Wire-Streams-Descriptor'></a>
### Descriptor `property`

##### Summary

Service descriptor

<a name='T-StreamsDB-Driver-Wire-Streams-StreamsClient'></a>
## StreamsClient `type`

##### Namespace

StreamsDB.Driver.Wire.Streams

##### Summary

Client for Streams

<a name='M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-Channel-'></a>
### #ctor(channel) `constructor`

##### Summary

Creates a new client for Streams

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| channel | [Grpc.Core.Channel](#T-Grpc-Core-Channel 'Grpc.Core.Channel') | The channel to use to make remote calls. |

<a name='M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-CallInvoker-'></a>
### #ctor(callInvoker) `constructor`

##### Summary

Creates a new client for Streams that uses a custom `CallInvoker`.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| callInvoker | [Grpc.Core.CallInvoker](#T-Grpc-Core-CallInvoker 'Grpc.Core.CallInvoker') | The callInvoker to use to make remote calls. |

<a name='M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor'></a>
### #ctor() `constructor`

##### Summary

Protected parameterless constructor to allow creation of test doubles.

##### Parameters

This constructor has no parameters.

<a name='M-StreamsDB-Driver-Wire-Streams-StreamsClient-#ctor-Grpc-Core-ClientBase-ClientBaseConfiguration-'></a>
### #ctor(configuration) `constructor`

##### Summary

Protected constructor to allow creation of configured clients.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| configuration | [Grpc.Core.ClientBase.ClientBaseConfiguration](#T-Grpc-Core-ClientBase-ClientBaseConfiguration 'Grpc.Core.ClientBase.ClientBaseConfiguration') | The client configuration. |

<a name='M-StreamsDB-Driver-Wire-Streams-StreamsClient-NewInstance-Grpc-Core-ClientBase-ClientBaseConfiguration-'></a>
### NewInstance() `method`

##### Summary

Creates a new instance of client from given `ClientBaseConfiguration`.

##### Parameters

This method has no parameters.

<a name='T-StreamsDB-Driver-StreamsDBClient'></a>
## StreamsDBClient `type`

##### Namespace

StreamsDB.Driver

##### Summary

Represents a client connection to a StreamsDB server.

<a name='M-StreamsDB-Driver-StreamsDBClient-Connect-System-String-'></a>
### Connect(connectionString) `method`

##### Summary

Connect to a StreamsDB server.

##### Returns



##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| connectionString | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The connection string that helps |

<a name='M-StreamsDB-Driver-StreamsDBClient-DB-System-String-'></a>
### DB(db) `method`

##### Summary

Get a handle to a database in StreamsDB.

##### Returns

A handle to the database.

##### Parameters

| Name | Type | Description |
| ---- | ---- | ----------- |
| db | [System.String](http://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k:System.String 'System.String') | The name of the database, leave empty to use the name from the connection string of the [StreamsDBClient](#T-StreamsDB-Driver-StreamsDBClient 'StreamsDB.Driver.StreamsDBClient'). |

<a name='T-StreamsDB-Driver-StreamsDBException'></a>
## StreamsDBException `type`

##### Namespace

StreamsDB.Driver

##### Summary

Base exception for all well known StreamsDB exceptions.

<a name='T-StreamsDB-Driver-Wire-SubscribeStreamRequest'></a>
## SubscribeStreamRequest `type`

##### Namespace

StreamsDB.Driver.Wire

<a name='F-StreamsDB-Driver-Wire-SubscribeStreamRequest-CountFieldNumber'></a>
### CountFieldNumber `constants`

##### Summary

Field number for the "count" field.

<a name='F-StreamsDB-Driver-Wire-SubscribeStreamRequest-DatabaseFieldNumber'></a>
### DatabaseFieldNumber `constants`

##### Summary

Field number for the "database" field.

<a name='F-StreamsDB-Driver-Wire-SubscribeStreamRequest-FromFieldNumber'></a>
### FromFieldNumber `constants`

##### Summary

Field number for the "from" field.

<a name='F-StreamsDB-Driver-Wire-SubscribeStreamRequest-StreamFieldNumber'></a>
### StreamFieldNumber `constants`

##### Summary

Field number for the "stream" field.
