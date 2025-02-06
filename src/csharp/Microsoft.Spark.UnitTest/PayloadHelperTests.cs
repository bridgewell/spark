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
                var args = new object[] { new double[] { 1.0d, 2.0d }.ToList() };
                PayloadHelper.BuildPayload(
                   ms,
                   isStatic,
                   _processId,
                   threadId,
                   classNameOrJvmObjectReference,
                   methodName,
                   args);

                Console.WriteLine(DisplayPayloadInfo(ms.ToArray()));
            }
        }


        /// <summary>
        /// Analyze the payload data and display the information.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string DisplayPayloadInfo(byte[] data)
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

            using (var ms = new MemoryStream(data))
            {
                var totalLength = SerDe.ReadInt32(ms);
                var isStaticMethod = SerDe.ReadBool(ms);
                var processId = SerDe.ReadInt32(ms);
                var threadId = SerDe.ReadInt32(ms);
                var classNameOrJvmObjectReference = SerDe.ReadString(ms);
                var methodName = SerDe.ReadString(ms);
                var argsLength = SerDe.ReadInt32(ms);
                var args = new string[argsLength];
                for (int i = 0; i < argsLength; ++i)
                {
                    args[i] = ReadArgObject(ms);
                }

                return $"totalLength: {totalLength},\r\nisStaticMethod: {isStaticMethod},\r\nprocessId: {processId},\r\n" +
                    $"threadId: {threadId},\r\nclassNameOrJvmObjectReference: {classNameOrJvmObjectReference},\r\n" +
                    $"methodName: {methodName},\r\nargsLength: {argsLength},\r\nargs:\r\n{string.Join("\r\n", args)}";
            }
        }

        public static string ReadArgObject(MemoryStream ms)
        {
            var typeidbyte = (byte)ms.ReadByte();
            var typeid = typeidMapping[typeidbyte];
            var resultType = typeid.ToString();
            return $"Type: {resultType}, Value: {ReadArgValue(ms, typeidbyte)}";
        }

        public static string ReadArgValue(MemoryStream ms, byte typeid)
        {
            switch (typeidMapping[typeid])
            {
                case TypeId.Int32:
                    return SerDe.ReadInt32(ms).ToString();
                case TypeId.Int64:
                    return SerDe.ReadInt64(ms).ToString();
                case TypeId.String:
                    return SerDe.ReadString(ms);
                case TypeId.Bool:
                    return SerDe.ReadBool(ms).ToString();
                case TypeId.Double:
                    return SerDe.ReadDouble(ms).ToString();
                case TypeId.Single:
                    return SerDe.ReadFloat(ms).ToString();
                case TypeId.Null:
                    return "null";
                case TypeId.Array:
                    var arrayTypeId = (byte)ms.ReadByte();
                    var arrayType = typeidMapping[arrayTypeId];
                    var arrayLength = SerDe.ReadInt32(ms);
                    var array = new string[arrayLength];
                    for (int i = 0; i < arrayLength; ++i)
                    {
                        array[i] = ReadArgValue(ms, arrayTypeId);
                    }
                    return $"[{arrayType}]: {string.Join(", ", array)}";
                default:
                    return "Unknown";
            }
        }

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

        private static Dictionary<byte, TypeId> typeidMapping = new Dictionary<byte, TypeId>() {
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


    }
}
