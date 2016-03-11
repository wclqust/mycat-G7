/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.config.model.crypto;

import org.opencloudb.crypto.function.AbstractCrytoAlgorithm;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;

/**
 * 加解密规则，column是用于加解密的数据库物理字段 可以有多个
 * 
 * @author G7_user
 * 
 */
public class CryptoConfig {
	private final String columns;//
	private final String functionName;
	private AbstractCrytoAlgorithm crytoAlgorithm;

	public CryptoConfig(String columns, String functionName) {
		if (functionName == null) {
			throw new IllegalArgumentException("functionName is null");
		}
		this.functionName = functionName;
		if (columns == null || columns.length() <= 0) {
			throw new IllegalArgumentException("no crypto column is found");
		}
		this.columns = columns;
	}

	/**
	 * @return unmodifiable, upper-case
	 */
	public String getColumns() {
		return columns;
	}

	public String getFunctionName() {
		return functionName;
	}

	public AbstractCrytoAlgorithm getCrytoAlgorithm() {
		return crytoAlgorithm;
	}

	public void setCrytoAlgorithm(AbstractCrytoAlgorithm crytoAlgorithm) {
		this.crytoAlgorithm = crytoAlgorithm;
	}

}