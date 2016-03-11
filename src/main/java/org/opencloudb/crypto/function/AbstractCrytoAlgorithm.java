package org.opencloudb.crypto.function;

import org.opencloudb.config.model.crypto.CrytoAlgorithm;

/**
 * 加密解密函数抽象类
 * 为了实现一个默认的加密解密的函数
 * 重写它以实现自己的加密解密规则
 * @author G7_user
 */
public abstract class AbstractCrytoAlgorithm implements CrytoAlgorithm {

	@Override
	public void init() {
	}

	
	
}
