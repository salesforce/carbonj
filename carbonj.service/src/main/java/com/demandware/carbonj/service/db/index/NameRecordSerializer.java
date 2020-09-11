/**
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.demandware.carbonj.service.db.index;

import com.demandware.carbonj.service.db.model.RetentionPolicy;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

class NameRecordSerializer
                implements RecordSerializer<String, NameRecord>
{
    private boolean longId;

    public NameRecordSerializer(boolean longId)
    {
        this.longId = longId;
    }
    public NameRecord toIndexEntry( byte[] keyBytes, byte[] valueBytes)
    {
        String name = key(keyBytes);
        return toIndexEntry( name, valueBytes);
    }

    // Format
    // ----------------------------------
    // type:byte 0 - non-leaf, 1 - leaf
    // id:int
    // Leaf:
    //   xFactor:double
    //   method:byte
    //   numArchives:byte
    //      name:string
    //   numChildren:int
    //   children: ...
    // Non-leaf:
    //   numChildren:int
    //   children: ...
    // Root:

    @Override
    public String key( byte[] keyBytes )
    {
        return new String(keyBytes, UTF_8);
    }

    @Override
    public NameRecord toIndexEntry( String key, byte[] valueBytes )
    {
        if(longId) {
            return toIndexEntryWithLongId(key, valueBytes);
        }
        return toIndexEntryWithIntId(key, valueBytes);
    }

    @Override
    public byte[] keyBytes(String key)
    {
        return key.getBytes( UTF_8 );
    }

    @Override
    public byte[] valueBytes(NameRecord e)
    {
        if(longId) {
            return valueBytesWithLongId(e);
        }
        return valueBytesWithIntId(e);
    }

    private  NameRecord toIndexEntryWithIntId( String key, byte[] valueBytes )
    {
        ByteArrayDataInput in = ByteStreams.newDataInput( valueBytes );
        int id = in.readInt();
        byte entryType = in.readByte();
        NameRecord e = new NameRecord( key, id, entryType == 1 );
        if( e.isLeaf() )
        {
            double xFactor = in.readDouble(); // no longer used
            byte methodId = in.readByte(); // no longer used
            int nArchives = in.readByte();
            List<RetentionPolicy> archivePolicies = new ArrayList<>(  );
            for(int i = 0; i < nArchives; i++)
            {
                //long precision = in.readLong();
                //long retention = in.readLong();
                archivePolicies.add( RetentionPolicy.getInstance( in.readUTF() ) );
            }
            Preconditions.checkState( archivePolicies.size() > 0 );
            e.setRetentionPolicies( archivePolicies );
        }
        else
        {
            int n = in.readInt();
            List<String> children = new ArrayList<>();
            for ( int i = 0; i < n; i++ )
            {
                children.add( in.readUTF() );
            }
            e.setChildren( children );
        }
        return e;
    }
    private NameRecord toIndexEntryWithLongId( String key, byte[] valueBytes )
    {

        ByteArrayDataInput in = ByteStreams.newDataInput( valueBytes );
        byte entryType = in.readByte(); // byte for versioning
        long id = in.readLong();
        NameRecord e = new NameRecord( key, id, entryType == 1 );
        if( e.isLeaf() )
        {
            double xFactor = in.readDouble(); // no longer used
            byte methodId = in.readByte(); // no longer used
            int nArchives = in.readByte();
            List<RetentionPolicy> archivePolicies = new ArrayList<>(  );
            for(int i = 0; i < nArchives; i++)
            {
                //long precision = in.readLong();
                //long retention = in.readLong();
                archivePolicies.add( RetentionPolicy.getInstance( in.readUTF() ) );
            }
            Preconditions.checkState( archivePolicies.size() > 0 );
            e.setRetentionPolicies( archivePolicies );
        }
        else
        {
            int n = in.readInt();
            List<String> children = new ArrayList<>();
            for ( int i = 0; i < n; i++ )
            {
                children.add( in.readUTF() );
            }
            e.setChildren( children );
        }
        return e;
    }

    private byte[] valueBytesWithIntId(NameRecord e)
    {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeInt( (int)e.getId() );
        if( e.isLeaf())
        {
            out.writeByte( 1 ); // leaf node type
            out.writeDouble( 0.0 ); // xFactor is not used and is always 0
            out.writeByte( 0 ); // aggregationPolicy is derived dynamically based on metric name and configuration file
            List<RetentionPolicy> archives = e.getRetentionPolicies();
            int nArchives = archives.size();
            out.writeByte(nArchives);
            archives.forEach( a -> out.writeUTF( a.name ) );
        }
        else
        {
            out.writeByte( 0 ); // non-leaf node type
            int n = e.getChildren().size();
            out.writeInt( n );
            e.getChildren().forEach( out::writeUTF );
        }

        return out.toByteArray();
    }
    private byte[] valueBytesWithLongId(NameRecord e)
    {
        // leaving the first byte for versioning and leafe node indication
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        if( e.isLeaf())
        {
            out.writeByte( 1 ); // leaf node type
            out.writeLong( e.getId() );
            out.writeDouble( 0.0 ); // xFactor is not used and is always 0
            out.writeByte( 0 ); // aggregationPolicy is derived dynamically based on metric name and configuration file
            List<RetentionPolicy> archives = e.getRetentionPolicies();
            int nArchives = archives.size();
            out.writeByte(nArchives);
            archives.forEach( a -> out.writeUTF( a.name ) );
        }
        else
        {
            out.writeByte( 0 ); // non-leaf node type
            out.writeLong( e.getId() );
            int n = e.getChildren().size();
            out.writeInt( n );
            e.getChildren().forEach( out::writeUTF );
        }

        return out.toByteArray();
    }

}
