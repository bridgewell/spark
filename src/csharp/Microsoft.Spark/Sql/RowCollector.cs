// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Network;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// RowCollector collects Row objects from a socket.
    /// </summary>
    internal sealed class RowCollector
    {
        /// <summary>
        /// Collects pickled row objects from the given socket.
        /// </summary>
        /// <param name="socket">Socket the get the stream from</param>
        /// <param name="expandall">
        ///    If row data contains rows inside, need set to true.
        ///    Otherwise the inner data will be RowContrustor (internal class).
        /// </param>
        /// <returns>Collection of row objects</returns>
        public IEnumerable<Row> Collect(ISocketWrapper socket, bool expandall = false)
        {
            Stream inputStream = socket.InputStream;

            int? length;
            while (((length = SerDe.ReadBytesLength(inputStream)) != null) &&
                (length.GetValueOrDefault() > 0))
            {
                object[] unpickledObjects =
                    PythonSerDe.GetUnpickledObjects(inputStream, length.GetValueOrDefault());

                foreach (object unpickled in unpickledObjects)
                {
                    var row = (unpickled as RowConstructor).GetRow();
                    yield return expandall ?
                        expandRowConstrutor(row) : row;
                }
            }
        }

        private object expandRowConstructorObj(object obj)
        {
            switch (obj)
            {
                case Row rowobj:
                    for (int i=0; i < rowobj.Values.Length; ++i)
                    {
                        rowobj.Values[i] = expandRowConstructorObj(rowobj.Values[i]);
                    }
                    return obj;                
                case RowConstructor rowconstobj:
                    return rowconstobj.GetRow();
                case ArrayList lst:
                    for (int i=0; i < lst.Count; ++i)
                    {
                        lst[i] = expandRowConstructorObj(lst[i]);
                    }
                    return obj;
                default:
                    return obj;
            }
        }

        private Row expandRowConstrutor(Row row)
        {
            for (int i = 0; i < row.Values.Length ; ++i)
            {
                row.Values[i] = expandRowConstructorObj(row.Values[i]);
            }
            return row;
        }
    }
}
