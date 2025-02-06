// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Interop.Ipc;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class PayloadHelperTests
    {
        [Fact]
        public void TestMethodAndFloatArgs()
        {
            using (var ms = new MemoryStream())
            {
                var isStatic = false;
                var _processId = 1;
                var threadId = 2;
                var classNameOrJvmObjectReference = "test";
                var methodName = "testMethod";
                var args = new object[] { new double[] { 1.0d, 2.0d }.ToList(), new float[] { 3.0f, 4.0f }.ToList(), 5d, 6f, "stringtest" };
                PayloadHelper.BuildPayload(
                   ms,
                   isStatic,
                   _processId,
                   threadId,
                   classNameOrJvmObjectReference,
                   methodName,
                   args);

                var payloadInfo = new PayLoadInfo();
                payloadInfo.ReadFromStream(new MemoryStream(ms.ToArray()));

                Assert.Equal(isStatic, payloadInfo.IsStaticMethod);
                Assert.Equal(_processId, payloadInfo.ProcessId);
                Assert.Equal(threadId, payloadInfo.ThreadId);
                Assert.Equal(classNameOrJvmObjectReference, payloadInfo.ClassNameOrJvmObjectReference);
                Assert.Equal(methodName, payloadInfo.MethodName);
                Assert.Equal(args.Length, payloadInfo.ArgsLength);
                Console.WriteLine(payloadInfo.Display());
            }
        }

        internal class PayLoadInfo
        {
            public int TotalLength { get; set; }
            public bool IsStaticMethod { get; set; }
            public int ProcessId { get; set; }
            public int ThreadId { get; set; }
            public string ClassNameOrJvmObjectReference { get; set; }
            public string MethodName { get; set; }
            public int ArgsLength { get; set; }
            public Arg[] Args { get; set; }


            public enum TypeId
            {
                Null,
                Int32,
                Int64,
                String,
                Bool,
                Double,
                Single,
                Date,
                Timestamp,
                JvmObject,
                ByteArray,
                DoubleArrayArray,
                SingleArrayArray,
                Array,
                Dictionary,
                RowArr,
                ObjectArr,
                ArrayListObject,
                SingleGenericRow,
            }

            public static Dictionary<byte, TypeId> typeidMapping = new Dictionary<byte, TypeId>() {
                { (byte)'n', TypeId.Null },
                { (byte)'i', TypeId.Int32 },
                { (byte)'g', TypeId.Int64 },
                { (byte)'c', TypeId.String },
                { (byte)'b', TypeId.Bool },
                { (byte)'d', TypeId.Double },
                { (byte)'f', TypeId.Single },
                { (byte)'D', TypeId.Date },
                { (byte)'t', TypeId.Timestamp },
                { (byte)'j', TypeId.JvmObject },
                { (byte)'r', TypeId.ByteArray },
                { (byte)'A', TypeId.DoubleArrayArray },
                { (byte)'F', TypeId.SingleArrayArray },
                { (byte)'l', TypeId.Array },
                { (byte)'e', TypeId.Dictionary },
                { (byte)'R', TypeId.RowArr },
                { (byte)'O', TypeId.ObjectArr },
                { (byte)'o', TypeId.ArrayListObject },
                { (byte)'s', TypeId.SingleGenericRow }
            };

            public class Arg
            {

                public override string ToString() => $"Type: {typeidMapping}, Value: {DisplayObj()}";

                public string DisplayObj()
                {
                    if (Obj == null)
                    {
                        return "null";
                    }
                    if (Obj is Array)
                    {
                        return string.Join(", ", (Obj as Arg[]).Select(a => a.DisplayObj()));
                    }
                    return Obj.ToString();
                }

                public object Obj { get; set; }

                public byte typeid { get; set; }
                public TypeId typeidMapping { get; set; }

                public static Arg ReadArgObject(MemoryStream ms)
                {
                    var ret = new Arg();
                    ret.typeid = (byte)ms.ReadByte();
                    return ReadArgArrayAsType(ms, ret.typeid);
                }

                public static Arg ReadArgArrayAsType(MemoryStream ms, byte typeid)
                {
                    var ret = new Arg();
                    ret.typeid = typeid;
                    ret.typeidMapping = PayLoadInfo.typeidMapping[ret.typeid];
                    ret.ReadArgValue(ms);
                    return ret;
                }

                public void ReadArgValue(MemoryStream ms)
                {
                    switch (PayLoadInfo.typeidMapping[typeid])
                    {
                        case TypeId.Int32:
                            Obj = SerDe.ReadInt32(ms);
                            break;
                        case TypeId.Int64:
                            Obj = SerDe.ReadInt64(ms);
                            break;
                        case TypeId.String:
                            Obj = SerDe.ReadString(ms);
                            break;
                        case TypeId.Bool:
                            Obj = SerDe.ReadBool(ms);
                            break;
                        case TypeId.Double:
                            Obj = SerDe.ReadDouble(ms);
                            break;
                        case TypeId.Single:
                            Obj = SerDe.ReadFloat(ms);
                            break;
                        case TypeId.Null:
                            Obj = null;
                            break;
                        case TypeId.Array:
                            var arrayTypeId = (byte)ms.ReadByte();
                            var arrayType = PayLoadInfo.typeidMapping[arrayTypeId];
                            var arrayLength = SerDe.ReadInt32(ms);
                            var array = new Arg[arrayLength];
                            for (int i = 0; i < arrayLength; ++i)
                            {
                                array[i] = ReadArgArrayAsType(ms, arrayTypeId);
                            }
                            Obj = array;
                            break;
                        default:
                            Obj = "Unknown";
                            break;
                    }
                }

            }

            public void ReadFromStream(MemoryStream ms)
            {
                // The payload data is a byte array, and the first 4 bytes are the total length of the payload data.
                // The following bytes are the payload data.
                // The payload data is composed of the following parts:
                // 1. isStaticMethod: 1 byte
                // 2. processId: 4 bytes
                // 3. threadId: 4 bytes
                // 4. classNameOrJvmObjectReference: string
                // 5. methodName: string
                // 6. args.Length: 4 bytes
                // 7. args: object[]

                TotalLength = SerDe.ReadInt32(ms);
                IsStaticMethod = SerDe.ReadBool(ms);
                ProcessId = SerDe.ReadInt32(ms);
                ThreadId = SerDe.ReadInt32(ms);
                ClassNameOrJvmObjectReference = SerDe.ReadString(ms);
                MethodName = SerDe.ReadString(ms);
                ArgsLength = SerDe.ReadInt32(ms);
                Args = new Arg[ArgsLength];
                for (int i = 0; i < ArgsLength; ++i)
                {
                    Args[i] = Arg.ReadArgObject(ms);
                }
            }

            public string Display()
            {
                var ArgDisplay = Args.Select(a => a.ToString());
                return $"TotalLength: {TotalLength}, IsStaticMethod: {IsStaticMethod}, ProcessId: {ProcessId}, ThreadId: {ThreadId}, ClassNameOrJvmObjectReference: {ClassNameOrJvmObjectReference}, MethodName: {MethodName}, ArgsLength: {ArgsLength}, Args: {string.Join(", ", ArgDisplay)}";
            }
        }

    }
}
