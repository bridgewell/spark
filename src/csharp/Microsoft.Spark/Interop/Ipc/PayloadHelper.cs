// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Helper to build the IPC payload for JVM calls from CLR.
    /// </summary>
    internal class PayloadHelper
    {
        private static readonly byte[] s_int32TypeId = new[] { (byte)'i' };
        private static readonly byte[] s_int64TypeId = new[] { (byte)'g' };
        private static readonly byte[] s_stringTypeId = new[] { (byte)'c' };
        private static readonly byte[] s_boolTypeId = new[] { (byte)'b' };
        private static readonly byte[] s_doubleTypeId = new[] { (byte)'d' };
        private static readonly byte[] s_signleTypeId = new[] { (byte)'f' };
        private static readonly byte[] s_dateTypeId = new[] { (byte)'D' };
        private static readonly byte[] s_timestampTypeId = new[] { (byte)'t' };
        private static readonly byte[] s_jvmObjectTypeId = new[] { (byte)'j' };
        private static readonly byte[] s_byteArrayTypeId = new[] { (byte)'r' };
        private static readonly byte[] s_doubleArrayArrayTypeId = new[] { (byte)'A' };
        private static readonly byte[] s_singleArrayArrayTypeId = new[] { (byte)'F' };
        private static readonly byte[] s_arrayTypeId = new[] { (byte)'l' };
        private static readonly byte[] s_dictionaryTypeId = new[] { (byte)'e' };
        private static readonly byte[] s_rowArrTypeId = new[] { (byte)'R' };
        private static readonly byte[] s_objectArrTypeId = new[] { (byte)'O' };

        private static readonly byte[] s_ArrayListObjectTypeId = new[] { (byte)'o' };

        private static readonly byte[] s_SingleGenericRowTypeId = new[] { (byte)'s' };

        private static readonly ConcurrentDictionary<Type, bool> s_isDictionaryTable =
            new ConcurrentDictionary<Type, bool>();

        internal static void BuildPayload(
            MemoryStream destination,
            bool isStaticMethod,
            int processId,
            int threadId,
            object classNameOrJvmObjectReference,
            string methodName,
            object[] args)
        {
            // Reserve space for total length.
            long originalPosition = destination.Position;
            destination.Position += sizeof(int);

            SerDe.Write(destination, isStaticMethod);
            SerDe.Write(destination, processId);
            SerDe.Write(destination, threadId);
            SerDe.Write(destination, classNameOrJvmObjectReference.ToString());
            SerDe.Write(destination, methodName);
            SerDe.Write(destination, args.Length);
            ConvertArgsToBytes(destination, args);

            // Write the length now that we've written out everything else.
            long afterPosition = destination.Position;
            destination.Position = originalPosition;
            SerDe.Write(destination, (int)afterPosition - sizeof(int));
            destination.Position = afterPosition;
        }

        internal static object ConvertListOfPrimitiveTypesToArray(object obj)
        {
            if (obj is IEnumerable<int> intList)
            {
                return intList.ToArray();
            }
            else if (obj is IEnumerable<long> longList)
            {
                return longList.ToArray();
            }
            else if (obj is IEnumerable<double> doubleList)
            {
                return doubleList.ToArray();
            }
            else if (obj is IEnumerable<float> floatList)
            {
                return floatList.ToArray();
            }
            else if (obj is IEnumerable<string> stringList)
            {
                return stringList.ToArray();
            }
            else if (obj is IEnumerable<bool> boolList)
            {
                return boolList.ToArray();
            }
            else
            {
                return obj;
            }
        }

        internal static void ConvertArgsToBytes(
            MemoryStream destination,
            object[] args,
            bool addTypeIdPrefix = true)
        {
            long posBeforeEnumerable, posAfterEnumerable;
            int itemCount;
            object[] convertArgs = null;

            foreach (object orgarg in args)
            {
                if (orgarg == null)
                {
                    destination.WriteByte((byte)'n');
                    continue;
                }

                var arg = ConvertListOfPrimitiveTypesToArray(orgarg);

                Type argType = arg.GetType();

                if (addTypeIdPrefix)
                {
                    SerDe.Write(destination, GetTypeId(argType));
                }

                switch (Type.GetTypeCode(argType))
                {
                    case TypeCode.UInt32:
                        SerDe.Write(destination, Convert.ToInt32(arg));
                        break;

                    case TypeCode.Int32:
                        SerDe.Write(destination, (int)arg);
                        break;

                    case TypeCode.UInt64:
                        SerDe.Write(destination, Convert.ToInt64(arg));
                        break;

                    case TypeCode.Int64:
                        SerDe.Write(destination, (long)arg);
                        break;

                    case TypeCode.String:
                        SerDe.Write(destination, (string)arg);
                        break;

                    case TypeCode.Boolean:
                        SerDe.Write(destination, (bool)arg);
                        break;

                    case TypeCode.Double:
                        SerDe.Write(destination, (double)arg);
                        break;

                    case TypeCode.Single:
                        SerDe.Write(destination, (float)arg);
                        break;

                    case TypeCode.Object:
                        switch (arg)
                        {
                            case byte[] argByteArray:
                                SerDe.Write(destination, argByteArray.Length);
                                SerDe.Write(destination, argByteArray);
                                break;

                            case int[] argInt32Array:
                                SerDe.Write(destination, s_int32TypeId);
                                SerDe.Write(destination, argInt32Array.Length);
                                foreach (int i in argInt32Array)
                                {
                                    SerDe.Write(destination, i);
                                }
                                break;

                            case long[] argInt64Array:
                                SerDe.Write(destination, s_int64TypeId);
                                SerDe.Write(destination, argInt64Array.Length);
                                foreach (long i in argInt64Array)
                                {
                                    SerDe.Write(destination, i);
                                }
                                break;

                            case double[] argDoubleArray:
                                SerDe.Write(destination, s_doubleTypeId);
                                SerDe.Write(destination, argDoubleArray.Length);
                                foreach (double d in argDoubleArray)
                                {
                                    SerDe.Write(destination, d);
                                }
                                break;

                            case float[] argFloatArray:
                                SerDe.Write(destination, s_signleTypeId);
                                SerDe.Write(destination, argFloatArray.Length);
                                foreach (float f in argFloatArray)
                                {
                                    SerDe.Write(destination, f);
                                }
                                break;

                            case float[][] argFloatArrayArray:
                                SerDe.Write(destination, s_singleArrayArrayTypeId);
                                SerDe.Write(destination, argFloatArrayArray.Length);
                                foreach (float[] floatArray in argFloatArrayArray)
                                {
                                    SerDe.Write(destination, floatArray.Length);
                                    foreach (float f in floatArray)
                                    {
                                        SerDe.Write(destination, f);
                                    }
                                }
                                break;

                            case double[][] argDoubleArrayArray:
                                SerDe.Write(destination, s_doubleArrayArrayTypeId);
                                SerDe.Write(destination, argDoubleArrayArray.Length);
                                foreach (double[] doubleArray in argDoubleArrayArray)
                                {
                                    SerDe.Write(destination, doubleArray.Length);
                                    foreach (double d in doubleArray)
                                    {
                                        SerDe.Write(destination, d);
                                    }
                                }
                                break;

                            case IEnumerable<byte[]> argByteArrayEnumerable:
                                SerDe.Write(destination, s_byteArrayTypeId);
                                WriteIEnumerableObjects(
                                    destination,
                                    argByteArrayEnumerable,
                                    (dest, b) =>
                                    {
                                        SerDe.Write(dest, b.Length);
                                        dest.Write(b, 0, b.Length);
                                    }
                                );
                                break;

                            case IEnumerable<string> argStringEnumerable:
                                SerDe.Write(destination, s_stringTypeId);
                                WriteIEnumerableObjects(
                                    destination,
                                    argStringEnumerable,
                                    (dest, s) => SerDe.Write(dest, s)
                                );
                                break;

                            case IEnumerable<IJvmObjectReferenceProvider> argJvmEnumerable:
                                SerDe.Write(destination, s_jvmObjectTypeId);
                                WriteIEnumerableObjects(
                                    destination,
                                    argJvmEnumerable,
                                    (dest, jvmObject) => SerDe.Write(dest, jvmObject.Reference.Id)
                                );
                                break;

                            case GenericRow singleRow:
                                SerDe.Write(destination, (int)singleRow.Values.Length);
                                ConvertArgsToBytes(destination, singleRow.Values, true);
                                break;

                            case IEnumerable<GenericRow> argRowEnumerable:
                                WriteIEnumerableObjects(
                                    destination,
                                    argRowEnumerable,
                                    (dest, r) =>
                                    {
                                        SerDe.Write(dest, (int)r.Values.Length);
                                        ConvertArgsToBytes(dest, r.Values, true);
                                    }
                                );
                                break;

                            case IEnumerable<object> argObjectEnumerable:
                                posBeforeEnumerable = destination.Position;
                                destination.Position += sizeof(int);
                                itemCount = 0;
                                if (convertArgs == null)
                                {
                                    convertArgs = new object[1];
                                }
                                foreach (object o in argObjectEnumerable)
                                {
                                    ++itemCount;
                                    convertArgs[0] = o;
                                    ConvertArgsToBytes(destination, convertArgs, true);
                                }
                                posAfterEnumerable = destination.Position;
                                destination.Position = posBeforeEnumerable;
                                SerDe.Write(destination, itemCount);
                                destination.Position = posAfterEnumerable;
                                break;

                            case var _ when IsDictionary(arg.GetType()):
                                // Generic dictionary, but we don't have it strongly typed as
                                // Dictionary<T,U>
                                var dictInterface = (IDictionary)arg;
                                var dict = new Dictionary<object, object>(dictInterface.Count);
                                IDictionaryEnumerator iter = dictInterface.GetEnumerator();
                                while (iter.MoveNext())
                                {
                                    dict[iter.Key] = iter.Value;
                                }

                                // Below serialization is corresponding to deserialization method
                                // ReadMap() of SerDe.scala.

                                // dictionary's length
                                SerDe.Write(destination, dict.Count);

                                // keys' data type
                                SerDe.Write(
                                    destination,
                                    GetTypeId(arg.GetType().GetGenericArguments()[0]));
                                // keys' length, same as dictionary's length
                                SerDe.Write(destination, dict.Count);
                                if (convertArgs == null)
                                {
                                    convertArgs = new object[1];
                                }
                                foreach (KeyValuePair<object, object> kv in dict)
                                {
                                    convertArgs[0] = kv.Key;
                                    // keys, do not need type prefix.
                                    ConvertArgsToBytes(destination, convertArgs, false);
                                }

                                // values' length, same as dictionary's length
                                SerDe.Write(destination, dict.Count);
                                foreach (KeyValuePair<object, object> kv in dict)
                                {
                                    convertArgs[0] = kv.Value;
                                    // values, need type prefix.
                                    ConvertArgsToBytes(destination, convertArgs, true);
                                }
                                break;

                            case IJvmObjectReferenceProvider argProvider:
                                SerDe.Write(destination, argProvider.Reference.Id);
                                break;

                            case Date argDate:
                                SerDe.Write(destination, argDate.ToString());
                                break;

                            case Timestamp argTimestamp:
                                SerDe.Write(destination, argTimestamp.GetIntervalInSeconds());
                                break;

                            case IEnumerable enumlist:
                                SerDe.Write(destination, s_ArrayListObjectTypeId);
                                WriteIEnumerableObjects(
                                    destination,
                                    enumlist,
                                    (dest, obj) => ConvertArgsToBytes(dest, new object[] { obj })
                                );
                                break;

                            default:
                                throw new NotSupportedException(
                                    string.Format($"Type {arg.GetType()} is not supported"));
                        }
                        break;
                }
            }
        }


        /// <summary>
        /// To write unknown count objects,
        /// We can reserved the counts data position at beginning,
        /// then write objects until we know the counts.
        /// Finally, write the count info back at the beginning posisiton.
        /// </summary>
        /// <param name="stream">The destination stream.</param>
        /// <param name="enumlist">enum list.</param>
        /// <param name="writefunc">iterate writefuncs</param>
        internal static void WriteIEnumerableObjects(MemoryStream stream, IEnumerable enumlist, Action<MemoryStream, object> writefunc)
        {
            var posBeforeEnumerable = stream.Position;
            stream.Position += sizeof(int);
            var itemCount = 0;
            foreach (var obj in enumlist)
            {
                itemCount++;
                writefunc(stream, obj);
            }
            var posAfterEnumerable = stream.Position;
            stream.Position = posBeforeEnumerable;
            SerDe.Write(stream, itemCount);
            stream.Position = posAfterEnumerable;
        }

        internal static void WriteIEnumerableObjects<T>(MemoryStream stream, IEnumerable<T> enumlist, Action<MemoryStream, T> writefunc)
        {
            var posBeforeEnumerable = stream.Position;
            stream.Position += sizeof(int);
            var itemCount = 0;
            foreach (var obj in enumlist)
            {
                itemCount++;
                writefunc(stream, obj);
            }
            var posAfterEnumerable = stream.Position;
            stream.Position = posBeforeEnumerable;
            SerDe.Write(stream, itemCount);
            stream.Position = posAfterEnumerable;
        }

        internal static byte[] GetTypeId(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Int32:
                case TypeCode.UInt32:
                    return s_int32TypeId;
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return s_int64TypeId;
                case TypeCode.String:
                    return s_stringTypeId;
                case TypeCode.Boolean:
                    return s_boolTypeId;
                case TypeCode.Double:
                    return s_doubleTypeId;
                case TypeCode.Single:
                    return s_signleTypeId;
                case TypeCode.Object:
                    if (typeof(IJvmObjectReferenceProvider).IsAssignableFrom(type))
                    {
                        return s_jvmObjectTypeId;
                    }

                    if (type == typeof(byte[]))
                    {
                        return s_byteArrayTypeId;
                    }

                    if (type == typeof(int[]) ||
                        type == typeof(long[]) ||
                        type == typeof(double[]) ||
                        type == typeof(double[][]) ||
                        type == typeof(float[]) ||
                        type == typeof(float[][]) ||
                        typeof(IEnumerable<byte[]>).IsAssignableFrom(type) ||
                        typeof(IEnumerable<string>).IsAssignableFrom(type))
                    {
                        return s_arrayTypeId;
                    }

                    if (IsDictionary(type))
                    {
                        return s_dictionaryTypeId;
                    }

                    if (typeof(IEnumerable<IJvmObjectReferenceProvider>).IsAssignableFrom(type))
                    {
                        return s_arrayTypeId;
                    }

                    if (type == typeof(GenericRow))
                    {
                        return s_SingleGenericRowTypeId;
                    }

                    if (typeof(IEnumerable<GenericRow>).IsAssignableFrom(type))
                    {
                        return s_rowArrTypeId;
                    }

                    if (typeof(IEnumerable<object>).IsAssignableFrom(type))
                    {
                        return s_objectArrTypeId;
                    }

                    if (typeof(Date).IsAssignableFrom(type))
                    {
                        return s_dateTypeId;
                    }

                    if (typeof(Timestamp).IsAssignableFrom(type))
                    {
                        return s_timestampTypeId;
                    }

                    if (typeof(IEnumerable).IsAssignableFrom(type))
                    {
                        return s_arrayTypeId;
                    }
                    break;
            }

            // TODO: Support other types.
            throw new NotSupportedException(string.Format("Type {0} not supported yet", type));
        }

        private static bool IsDictionary(Type type)
        {
            if (!s_isDictionaryTable.TryGetValue(type, out var isDictionary))
            {
                s_isDictionaryTable[type] = isDictionary =
                    type.GetInterfaces().Any(
                        i => i.IsGenericType &&
                            (i.GetGenericTypeDefinition() == typeof(IDictionary<,>)));
            }
            return isDictionary;
        }
    }
}
