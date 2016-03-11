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
package org.opencloudb.crypto.function;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.opencloudb.config.model.crypto.CrytoAlgorithm;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.crypto.exception.CrytoException;
import org.opencloudb.util.DecryptUtil;

/**
 * 对称加密实现
 * 
 * @author G7_user
 * 
 */
public class SymmetricCryto extends AbstractCrytoAlgorithm implements
		CrytoAlgorithm {
	private String type = "AES";// 加密类型
	private int size = 128;// 解密位数
	private String part1Url;// 密钥第一部分路径
	private String part2Url;// 密钥第二部分路径
	private SecretKey secretKey;// 密码对象

	@Override
	public void init() {

		secretKey = genKey(type, size, part1Url, part2Url);
	}

	private SecretKey genKey(String type, int size, String part1Url,
			String part2Url) {
		try {
			if (secretKey == null) {
				KeyGenerator keyGen = KeyGenerator.getInstance(type);
				SecureRandom secureRandom = SecureRandom
						.getInstance("SHA1PRNG");
				String seed = loadSeed(part1Url, part2Url);
				secureRandom.setSeed(seed.getBytes());
				keyGen.init(size, secureRandom);// 设置算法密钥位数
				secretKey = keyGen.generateKey();
			}
		} catch (NoSuchAlgorithmException e) {
			throw new ConfigException("SymmetricCryto init failed!!!", e);
		} catch (Exception e) {
			throw new ConfigException("SymmetricCryto init failed!!!", e);
		}
		System.out.println("key-------------------------------" + secretKey);
		return secretKey;
	}

	private String loadSeed(String part1Url2, String part2Url2) {
		// 默认返回值
		return "qazqwe123";
	}

	/*
	 * private void initialize() { }
	 */

	static class LongRange {
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}

	@Override
	public byte[] decrypt(byte[] base642byte) {
		byte[] bytes = base642byte;
		try {
			Cipher cipher = Cipher.getInstance(type);
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
			System.out.println(DecryptUtil.byteArrayToInt(DecryptUtil.subByte(
					base642byte, 0, 4)));
			System.out.println(DecryptUtil.byteArrayToInt(DecryptUtil.subByte(
					base642byte, 4, 8)));
			bytes = cipher.doFinal(DecryptUtil.subByte(base642byte, 8,
					base642byte.length));
			return bytes;// 如果失败原值返回
		} catch (Exception e) {
			throw new CrytoException("解密失败!!!", e);
		}
	}
	@Override
	public byte[] encrypt(String oldvalue) {
		byte[] encryptBytes = oldvalue.getBytes();
		try {
			Cipher cipher = Cipher.getInstance(type);
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			byte[] bytes = cipher.doFinal(oldvalue.getBytes());
			// 再加上加密数据的坐标
			byte[] keypart1 = DecryptUtil.intToByteArray(1);
			byte[] keypart2 = DecryptUtil.intToByteArray(888);
			encryptBytes = DecryptUtil.byteMerger(
					DecryptUtil.byteMerger(keypart1, keypart2), bytes);
			return encryptBytes;
		} catch (Exception e) {
			throw new CrytoException("加密失败!!!", e);
		} finally {
			
		}
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

}