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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.opencloudb.MycatServer;
import org.opencloudb.config.model.crypto.CrytoAlgorithm;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.crypto.exception.CrytoException;
import org.opencloudb.s3.connector.S3ServiceConnectorCreator;
import org.opencloudb.s3.connector.S3ServiceInfo;
import org.opencloudb.s3.modal.SecretKeyModal;
import org.opencloudb.s3.repository.S3;
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
	private String part2Value = null;
	private static String serviceName = "ServiceName=";
	private static String dbName = "DBName=";
	private static String tableName = "TableName=";
	private static String columnName = "ColumnName=";
	private static String version = "Version=";
	private ConcurrentHashMap<String, SecretKey> secretKeyValueMap = new ConcurrentHashMap<String, SecretKey>();
	private MessageDigest md5;

	@Override
	public void init() {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new ConfigException("SymmetricCryto init failed!!!", e);
		}
		// 应用初始化化时 初始化s3的所有的key，如果变更的话，要注意更新缓存
		// secretKey = genKey(type, size, part1Url, part2Url);
	}

	private SecretKey genKey(String type, int size, String part1Url,
			String part1Key, String part2value) {
		SecretKey secretKey = null;
		try {

			KeyGenerator keyGen = KeyGenerator.getInstance(type);
			SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
			String seed = loadSeed(part1Url, part1Key, part2value);
			secureRandom.setSeed(seed.getBytes());
			keyGen.init(size, secureRandom);// 设置算法密钥位数
			secretKey = keyGen.generateKey();

		} catch (NoSuchAlgorithmException e) {
			throw new ConfigException("SymmetricCryto init failed!!!", e);
		} catch (Exception e) {
			throw new ConfigException("SymmetricCryto init failed!!!", e);
		}
		System.out.println("key-------------------------------" + secretKey);
		return secretKey;
	}

	private String loadSeed(String part1Url, String part1Key, String part2value) {
		String returnStr = "qazqwe123";
		// AWS S3获取第一部分的数据
		String part1UUID = loadPart1Seed(part1Key, part1Url);
		returnStr = part1UUID + part2value;
		return returnStr;// 默认的返回值
	}

	/**
	 * 
	 * @param key
	 * @return MD5摘要后的路径 s3存储key格式 //
	 *         ServiceName=XXXXX/DBName=XXXXX/TableName=XXXXX
	 *         /ColumnName=XXXX/Version=1
	 */
	private String getPart1Key(String key) {
		StringBuilder sb = new StringBuilder();
		// *************start************
		sb.append("/");
		// *************serviceName************
		sb.append(SymmetricCryto.serviceName);
		sb.append(MycatServer.getInstance().getConfig().getSystem()
				.getServiceName());
		sb.append("/");
		String[] temp = key.split("\\|");
		// *************dbName************
		sb.append(SymmetricCryto.dbName);
		sb.append(temp[0]);
		sb.append("/");
		// *************tableName************
		sb.append(SymmetricCryto.tableName);
		sb.append(temp[1]);
		sb.append("/");
		// *************columuName************
		sb.append(SymmetricCryto.columnName);
		sb.append(temp[2]);
		sb.append("/");
		// *************versionNo************
		sb.append(SymmetricCryto.version);
		sb.append(temp[3]);
		byte[] digest = null;
		try {
			digest = md5.digest(sb.toString().getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			throw new ConfigException("md5 getPart1Key failed!!!", e);
		}
		return DecryptUtil.byte2base64(digest);
	}

	private String loadPart2Seed(String part2Url) {
		// 默认返回123
		if (part2Value == null)
			part2Value = "123";
		return "123";
	}

	private String loadPart1Seed(String part1key, String part1Url2) {
		String part1UUID = null;

		S3 s3 = S3ServiceConnectorCreator.create(S3ServiceInfo.bucket);
		try {
			part1UUID = s3.get(new SecretKeyModal(part1key, null));
			if (part1UUID == null || "".equals(part1UUID.trim())) {
				part1UUID = UUID.randomUUID().toString();// 默认的第一部分的密钥
				s3.put(new SecretKeyModal(part1key, part1UUID));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			if (part1UUID == null || "".equals(part1UUID.trim())) {
				part1UUID = UUID.randomUUID().toString();// 默认的第一部分的密钥
				s3.put(new SecretKeyModal(part1key, part1UUID));
			}
		}

		// part1UUID = UUID.randomUUID().toString();// 默认的第一部分的密钥
		return part1UUID;
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
	public byte[] decrypt(byte[] base642byte, String key) {
		String part1value = DecryptUtil.byteArrayToInt(DecryptUtil.subByte(
				base642byte, 0, 4)) + "";
		String part2value = DecryptUtil.byteArrayToInt(DecryptUtil.subByte(
				base642byte, 4, 8)) + "";
		System.out.println(part1value + "--" + part2value);
		byte[] bytes = base642byte;
		try {
			Cipher cipher = Cipher.getInstance(type);
			SecretKey secretKey = getSecretKey(getPart1Key(key + "|"
					+ part1value),part2value);
			cipher.init(Cipher.DECRYPT_MODE, secretKey);
			bytes = cipher.doFinal(DecryptUtil.subByte(base642byte, 8,
					base642byte.length));
			return bytes;// 如果失败原值返回
		} catch (Exception e) {
			throw new CrytoException("解密失败!!!", e);
		}
	}

	@Override
	public byte[] encrypt(String oldvalue, String key) {
		byte[] encryptBytes = oldvalue.getBytes();
		try {
			String crytoPart1VN = MycatServer.getInstance().getConfig()
					.getSystem().getCrytoPart1VN();
			Cipher cipher = Cipher.getInstance(type);
			SecretKey secretKey = getSecretKey(getPart1Key(key + "|"
					+ crytoPart1VN), loadPart2Seed(part2Url));
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			byte[] bytes = cipher.doFinal(oldvalue.getBytes());
			// 再加上加密数据的坐标
			byte[] keypart1 = DecryptUtil.intToByteArray(Integer
					.valueOf(crytoPart1VN));
			byte[] keypart2 = DecryptUtil.intToByteArray(Integer
					.valueOf(part2Value));
			encryptBytes = DecryptUtil.byteMerger(
					DecryptUtil.byteMerger(keypart1, keypart2), bytes);
			return encryptBytes;
		} catch (Exception e) {
			throw new CrytoException("加密失败!!!", e);
		} finally {

		}
	}

	private SecretKey getSecretKey(String key1, String key2) {
		SecretKey secretKey = secretKeyValueMap.get(key1 + "-" + key2);
		if (secretKey == null) {
			secretKey = genKey(type, size, part1Url, key1, key2);
			secretKeyValueMap.putIfAbsent(key1 + "-" + key2, secretKey);
		}

		return secretKey;
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