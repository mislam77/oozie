/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.persistence.Embeddable;

import org.apache.hadoop.io.Writable;
import org.apache.oozie.util.WritableUtils;

/**
 * The composite primary key for the BundleActionBean Entity.
 *
 */
@Embeddable
public class BundleActionId implements Writable {
    private String bundleId = null;
    private String coordName = null;

    /**
     * @param bundleId the bundleId to set
     */
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    /**
     * @return the bundleId
     */
    public String getBundleId() {
        return bundleId;
    }

    /**
     * @param coordName the coordName to set
     */
    public void setCoordName(String coordName) {
        this.coordName = coordName;
    }

    /**
     * @return the coordName
     */
    public String getCoordName() {
        return coordName;
    }

    @Override
    public int hashCode() {
        return new String(bundleId + coordName).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BundleActionId) {
            return bundleId.equals(((BundleActionId) obj).getBundleId()) && coordName.equals(((BundleActionId) obj).getCoordName());
        } else {
            return false;
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setBundleId(WritableUtils.readStr(dataInput));
        setCoordName(WritableUtils.readStr(dataInput));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeStr(dataOutput, getBundleId());
        WritableUtils.writeStr(dataOutput, getCoordName());
    }
}
