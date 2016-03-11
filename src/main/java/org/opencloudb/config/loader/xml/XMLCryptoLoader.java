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
package org.opencloudb.config.loader.xml;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLSyntaxErrorException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.opencloudb.config.model.crypto.CryptoConfig;
import org.opencloudb.config.model.crypto.TableCryptoConfig;
import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.config.model.rule.TableRuleConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.config.util.ConfigUtil;
import org.opencloudb.config.util.ParameterMapping;
import org.opencloudb.crypto.function.AbstractCrytoAlgorithm;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.util.SplitUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 加密功能配置文件加载器
 * 
 * @author G7_user
 * 
 */
@SuppressWarnings("unchecked")
public class XMLCryptoLoader {
	private final static String DEFAULT_DTD = "/crypto.dtd";
	private final static String DEFAULT_XML = "/crypto.xml";

	private final Map<String, TableCryptoConfig> tableCryptos;
	private final Map<String, AbstractCrytoAlgorithm> functions;

	public XMLCryptoLoader(String cryptoFile) {
		// this.rules = new HashSet<RuleConfig>();
		this.tableCryptos = new HashMap<String, TableCryptoConfig>();
		this.functions = new HashMap<String, AbstractCrytoAlgorithm>();
		load(DEFAULT_DTD, cryptoFile == null ? DEFAULT_XML : cryptoFile);
	}

	public XMLCryptoLoader() {
		this(null);
	}

	public Map<String, TableCryptoConfig> getTableCryptos() {
		return (Map<String, TableCryptoConfig>) (tableCryptos.isEmpty() ? Collections
				.emptyMap() : tableCryptos);
	}

	private void load(String dtdFile, String xmlFile) {
		InputStream dtd = null;
		InputStream xml = null;
		try {
			dtd = XMLCryptoLoader.class.getResourceAsStream(dtdFile);
			xml = XMLCryptoLoader.class.getResourceAsStream(xmlFile);
			Element root = ConfigUtil.getDocument(dtd, xml)
					.getDocumentElement();
			loadFunctions(root);
			loadTableCryptos(root);
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new ConfigException(e);
		} finally {
			if (dtd != null) {
				try {
					dtd.close();
				} catch (IOException e) {
				}
			}
			if (xml != null) {
				try {
					xml.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private void loadTableCryptos(Element root) throws SQLSyntaxErrorException {
		NodeList list = root.getElementsByTagName("crypto");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			Node node = list.item(i);
			if (node instanceof Element) {
				Element e = (Element) node;
				String name = e.getAttribute("name");
				if (tableCryptos.containsKey(name)) {
					throw new ConfigException("table rule " + name
							+ " duplicated!");
				}
				NodeList columnNodes = e.getElementsByTagName("column");

				CryptoConfig[] cryptoConfig = loadCryptoConfig(columnNodes);

				tableCryptos.put(name,
						new TableCryptoConfig(name, cryptoConfig));
			}
		}
	}

	private CryptoConfig[] loadCryptoConfig(NodeList columnNodes) {
		CryptoConfig[] returnArr = new CryptoConfig[columnNodes.getLength()];
		for (int i = 0; i < columnNodes.getLength(); i++) {

			Element column = (Element) columnNodes.item(0);
			String name = column.getAttribute("name");
			String funName = column.getAttribute("algorithm");
			AbstractCrytoAlgorithm func = functions.get(funName);
			if (func == null) {
				throw new ConfigException("can't find function of name :"
						+ funName);
			}
			CryptoConfig cryptoConfig = new CryptoConfig(name, funName);
			cryptoConfig.setCrytoAlgorithm(func);
			returnArr[i] = cryptoConfig;
		}
		return returnArr;
	}

	private void loadFunctions(Element root) throws ClassNotFoundException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		NodeList list = root.getElementsByTagName("function");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			Node node = list.item(i);
			if (node instanceof Element) {
				Element e = (Element) node;
				String name = e.getAttribute("name");
				if (functions.containsKey(name)) {
					throw new ConfigException("rule function " + name
							+ " duplicated!");
				}
				String clazz = e.getAttribute("class");
				AbstractCrytoAlgorithm function = createFunction(name, clazz);
				ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
				function.init();
				functions.put(name, function);
			}
		}
	}

	private AbstractCrytoAlgorithm createFunction(String name, String clazz)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException {
		Class<?> clz = Class.forName(clazz);
		if (!AbstractCrytoAlgorithm.class.isAssignableFrom(clz)) {
			throw new IllegalArgumentException("rule function must implements "
					+ AbstractCrytoAlgorithm.class.getName() + ", name=" + name);
		}
		return (AbstractCrytoAlgorithm) clz.newInstance();
	}

}