package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.crypto.CrytoAlgorithm;
import org.opencloudb.mpp.RangeValue;
import org.opencloudb.parser.druid.DruidParser;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.parser.druid.MycatSchemaStatVisitor;
import org.opencloudb.parser.druid.RouteCalculateUnit;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.util.DecryptUtil;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.util.JdbcConstants;

/**
 * 对SQLStatement解析 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 * 有些只能通过statement解析才能得到所有信息 有些需要通过两种方式解析才能得到完整信息
 * 
 * @author wang.dw
 * 
 */
public class DefaultDruidParser implements DruidParser {
	protected static final Logger LOGGER = Logger
			.getLogger(DefaultDruidParser.class);
	/**
	 * 解析得到的结果
	 */
	protected DruidShardingParseInfo ctx;

	private Map<String, String> tableAliasMap = new HashMap<String, String>();

	private List<Condition> conditions = new ArrayList<Condition>();

	public Map<String, String> getTableAliasMap() {
		return tableAliasMap;
	}

	public List<Condition> getConditions() {
		return conditions;
	}

	/**
	 * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
	 * 
	 * @param schema
	 * @param stmt
	 */
	public void parser(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt, String originSql, LayerCachePool cachePool,
			MycatSchemaStatVisitor schemaStatVisitor)
			throws SQLNonTransientException {
		ctx = new DruidShardingParseInfo();
		//System.out.println(stmt.toString());
		// 设置为原始sql，如果有需要改写sql的，可以通过修改SQLStatement中的属性，然后调用SQLStatement.toString()得到改写的sql
		ctx.setSql(originSql);
		// 通过visitor解析
		visitorParse(rrs, stmt, schemaStatVisitor);
		// 通过Statement解析
		statementParse(schema, rrs, stmt);

		// 改写sql：如insert语句主键自增长的可以
		changeSql(schema, rrs, stmt, cachePool);

		RouteResultsetNode[] nodes = rrs.getNodes();
		for (int i = 0; nodes != null && i < nodes.length; i++) {
			nodes[i].setSchema(schema.getName().toLowerCase());
		}
	}

	/**
	 * 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析）
	 * 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
	 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt) throws SQLNonTransientException {

	}

	/**
	 * 改写sql：如insert是
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt, LayerCachePool cachePool)
			throws SQLNonTransientException {

		if (stmt instanceof MySqlInsertStatement) {// 新增的情况
			MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;
			List<SQLExpr> cloumnslist = insertStmt.getColumns();
			List<ValuesClause> vclist = insertStmt.getValuesList();

			String tablename = insertStmt.getTableName().getSimpleName()
					.replaceAll("\\`", "").toString().toLowerCase();
			/*
			 * System.out.println("列名：" + cloumnslist + "||" + "表名：" + tablename
			 * + "||" + "列值：" + vclist);
			 */

			ValuesClause vc = vclist.get(0);
			List<SQLExpr> sqlexprlist = vc.getValues();

			for (int i = 0; i < sqlexprlist.size(); i++) {
				SQLExpr cuurSQLExpr = cloumnslist.get(i);
				String key = schema.getName().toLowerCase() + "|" + tablename
						+ "|" + cuurSQLExpr.toString().toLowerCase();
				SQLCharExpr sqlCharExpr = encryptAction(key, sqlexprlist.get(i)
						.toString());
				if (sqlCharExpr != null) {
					sqlexprlist.set(i, sqlCharExpr);
				}

			}
			// System.out.println("新列值：" + vclist);
			insertStmt.setValuesList(vclist);

		} else if (stmt instanceof MySqlUpdateStatement) {// 修改的情况
			MySqlUpdateStatement updateStmt = (MySqlUpdateStatement) stmt;
			List<SQLUpdateSetItem> updatesetitemlist = updateStmt.getItems();
			String tablename = updateStmt.getTableName().toString()
					.replaceAll("\\`", "").toLowerCase();
			for (int i = 0; i < updatesetitemlist.size(); i++) {
				SQLUpdateSetItem cuurSQLUpdateSetItem = updatesetitemlist
						.get(i);
				String key = schema.getName().toLowerCase()
						+ "|"
						+ tablename
						+ "|"
						+ cuurSQLUpdateSetItem.getColumn().toString()
								.toLowerCase();
				SQLCharExpr sqlCharExpr = encryptAction(key,
						cuurSQLUpdateSetItem.getValue().toString());
				if (sqlCharExpr != null) {
					SQLUpdateSetItem newitem = new SQLUpdateSetItem();
					newitem.setColumn(new SQLIdentifierExpr(
							cuurSQLUpdateSetItem.getColumn().toString()));
					newitem.setValue(sqlCharExpr);
					updatesetitemlist.set(i, newitem);
				}

			}
		} else if (stmt instanceof SQLSelectStatement) {// 查询的情况

			SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
			SQLSelectQueryBlock sqlSelectQuery = (SQLSelectQueryBlock) selectStmt
					.getSelect().getQuery();
			SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlSelectQuery
					.getWhere();
			if (binaryOpExpr != null) {
				List<MatchWhereVo> matchWhereList = matchWhere(binaryOpExpr,
						schema, new ArrayList<MatchWhereVo>());
				for (int i = 0; i < matchWhereList.size(); i++) {
					MatchWhereVo matchWhereVo = matchWhereList.get(i);
					SQLCharExpr sqlCharExpr = encryptAction(matchWhereVo.key,
							matchWhereVo.oldvalue);
					if (sqlCharExpr != null)
						matchWhereVo.sqlBinaryOpExpr.setRight(sqlCharExpr);
				}

			}

		}
		ctx.setSql(stmt.toString());// ctx中重新设置sql语句 非常关键
		rrs.setStatement(stmt.toString());
	}

	private List<MatchWhereVo> matchWhere(SQLBinaryOpExpr binaryOpExpr,
			SchemaConfig schema, ArrayList<MatchWhereVo> arrayList) {
		SQLExpr left = binaryOpExpr.getLeft();
		SQLExpr right = binaryOpExpr.getRight();

		if (right instanceof SQLBinaryOpExpr) {
			SQLExpr left2 = ((SQLBinaryOpExpr) right).getLeft();
			if (left2 instanceof SQLIdentifierExpr) {
				String columu = left2.getAttribute("_column_").toString();
				String[] columuArr = columu.split("\\.");
				if (columuArr.length > 0) {
					String key = schema.getName().toLowerCase() + "|"
							+ columuArr[0].toLowerCase() + "|"
							+ columuArr[1].toLowerCase();
					String oldvalue = ((SQLBinaryOpExpr) right).getRight()
							.toString();
					arrayList
							.add(new MatchWhereVo(key, oldvalue, (SQLBinaryOpExpr) right));

				}
			}

		}
		if (left instanceof SQLIdentifierExpr) {
			String columu = left.getAttribute("_column_").toString();
			String[] columuArr = columu.split("\\.");
			if (columuArr.length > 0) {
				String key = schema.getName().toLowerCase() + "|"
						+ columuArr[0].toLowerCase() + "|"
						+ columuArr[1].toLowerCase();
				String oldvalue = binaryOpExpr.getRight().toString();
				arrayList.add(new MatchWhereVo(key, oldvalue, binaryOpExpr));

			}

		} else if (left instanceof SQLBinaryOpExpr) {
			return matchWhere((SQLBinaryOpExpr) left, schema, arrayList);
		}

		return arrayList;
	}

	private SQLCharExpr encryptAction(String key, String oldvalue) {
		CrytoAlgorithm crytoAlgorithm = MycatServer.getInstance()
				.getCrytoCacheMap().get(key);
		if (crytoAlgorithm != null) {// 符合条件
			oldvalue = oldvalue.substring(1, oldvalue.length() - 1);

			byte[] enbytes = crytoAlgorithm.encrypt(oldvalue,key);

			return new SQLCharExpr(DecryptUtil.byte2base64(enbytes));
		}
		return null;
	}

	/**
	 * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
	 * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
	 * 
	 * @param stmt
	 */
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt,
			MycatSchemaStatVisitor visitor) throws SQLNonTransientException {

		stmt.accept(visitor);

		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if (visitor.hasOrCondition()) {// 包含or语句
			// TODO
			// 根据or拆分
			mergedConditionList = visitor.splitConditions();
		} else {// 不包含OR语句
			mergedConditionList.add(visitor.getConditions());
		}

		if (visitor.getAliasMap() != null) {
			for (Map.Entry<String, String> entry : visitor.getAliasMap()
					.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				if (key != null && key.indexOf("`") >= 0) {
					key = key.replaceAll("`", "");
				}
				if (value != null && value.indexOf("`") >= 0) {
					value = value.replaceAll("`", "");
				}
				// 表名前面带database的，去掉
				if (key != null) {
					int pos = key.indexOf(".");
					if (pos > 0) {
						key = key.substring(pos + 1);
					}
				}

				if (key.equals(value)) {
					ctx.addTable(key.toUpperCase());
				}
				// else {
				// tableAliasMap.put(key, value);
				// }
				tableAliasMap.put(key.toUpperCase(), value);
			}
			visitor.getAliasMap().putAll(tableAliasMap);
			ctx.setTableAliasMap(tableAliasMap);
		}
		ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(visitor,
				mergedConditionList));
	}

	private List<RouteCalculateUnit> buildRouteCalculateUnits(
			SchemaStatVisitor visitor, List<List<Condition>> conditionList) {
		List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();
		// 遍历condition ，找分片字段
		for (int i = 0; i < conditionList.size(); i++) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			for (Condition condition : conditionList.get(i)) {
				List<Object> values = condition.getValues();
				if (values.size() == 0) {
					break;
				}
				if (checkConditionValues(values)) {
					String columnName = StringUtil.removeBackquote(condition
							.getColumn().getName().toUpperCase());
					String tableName = StringUtil.removeBackquote(condition
							.getColumn().getTable().toUpperCase());

					if (visitor.getAliasMap() != null
							&& visitor.getAliasMap().get(tableName) != null
							&& !visitor.getAliasMap().get(tableName)
									.equals(tableName)) {
						tableName = visitor.getAliasMap().get(tableName);
					}

					if (visitor.getAliasMap() != null
							&& visitor.getAliasMap().get(
									condition.getColumn().getTable()
											.toUpperCase()) == null) {// 子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
						continue;
					}

					String operator = condition.getOperator();

					// 只处理between ,in和=3中操作符
					if (operator.equals("between")) {
						RangeValue rv = new RangeValue(values.get(0),
								values.get(1), RangeValue.EE);
						routeCalculateUnit.addShardingExpr(
								tableName.toUpperCase(), columnName, rv);
					} else if (operator.equals("=")
							|| operator.toLowerCase().equals("in")) { // 只处理=号和in操作符,其他忽略
						routeCalculateUnit.addShardingExpr(
								tableName.toUpperCase(), columnName,
								values.toArray());
					}
				}
			}
			retList.add(routeCalculateUnit);
		}
		return retList;
	}

	private boolean checkConditionValues(List<Object> values) {
		for (Object value : values) {
			if (value != null && !value.toString().equals("")) {
				return true;
			}
		}
		return false;
	}

	public DruidShardingParseInfo getCtx() {
		return ctx;
	}

	static class MatchWhereVo {
		public final String key;
		public final String oldvalue;
		public final SQLBinaryOpExpr sqlBinaryOpExpr;

		public MatchWhereVo(String key, String oldvalue,
				SQLBinaryOpExpr sqlBinaryOpExpr) {
			super();
			this.key = key;
			this.oldvalue = oldvalue;
			this.sqlBinaryOpExpr = sqlBinaryOpExpr;
		}

	}
}
