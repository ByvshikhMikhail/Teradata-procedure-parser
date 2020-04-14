

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.stmt.mssql.TMssqlDeclare;
import gudusoft.gsqlparser.stmt.mssql.TMssqlDropTable;
import gudusoft.gsqlparser.stmt.teradata.TTeradataLock;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Analyze_SP {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java Analyze_SP scriptfile [/o <output file path>] [/d <csv delimiter character>]");
            return;
        }
//        System.out.println(args.toString());
        List array = Arrays.asList(args);
        List<File> files = new ArrayList<File>();
        for (int i = 0; i < array.size(); i++) {
            File file = new File(array.get(i).toString());
            if (file.exists())
                files.add(file);
            else
                break;
        }

        String outputFile = null;
        int index = array.indexOf("/o");
        if (index != -1 && args.length > index + 1) {
            outputFile = args[index + 1];
        }
        String delimiter = "|";
        index = array.indexOf("/d");
        if (index != -1 && args.length > index + 1) {
            delimiter = args[index + 1];
        }

        Analyze_SP impact = new Analyze_SP(files, delimiter);
        impact.analyzeSQL();

        PrintStream writer = null;
        if (outputFile != null) {
            try {
                writer = new PrintStream(new FileOutputStream(outputFile,
                        false));
                System.setOut(writer);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

        }

        System.out.println("Number"
                + delimiter
                + "Purpose"
                + delimiter
                + "Target schema"
                + delimiter
                + "Target table"
                + delimiter
                + "Type target"
                + delimiter
                + "Target columns"
                + delimiter
                + "Src_schema"
                + delimiter
                + "Src_table"
                + delimiter
                + "Alias"
                + delimiter
                + "Type"
                + delimiter
                + "Join type"
                + delimiter
                + "on Condition"
                + delimiter
                + "Where"
                + delimiter
                + "Set"
                + delimiter
                + "Src file name"
                + delimiter
                + "Call Params"
                + delimiter
                + "Columns"
                + delimiter
                + "Max_cdc_date");
        System.out.println(impact.getDBObjectRelationsAnalysisResult());


        if (writer != null) {
            writer.close();
        }

    }

    private StringBuilder relationBuffer = new StringBuilder();
    private Map spInfoMap = new HashMap();
    private Map stmtNumbers = new HashMap();
    private List<String> files = new ArrayList<String>();
    private String delimiter;

    public Analyze_SP(List<File> sqlFiles, String delimiter) {
        this.delimiter = delimiter;
        if (sqlFiles.size() > 0) {
            for (int i = 0; i < sqlFiles.size(); i++) {
                files.add(sqlFiles.get(i).getAbsolutePath());
                spInfo sp = new spInfo();
                sp.file = sqlFiles.get(i).getName();
                String fileAsString = null;
                try {
                    InputStream is = new FileInputStream(sqlFiles.get(i).getAbsolutePath());
                    BufferedReader buf = new BufferedReader(new InputStreamReader(is));

                    String line = buf.readLine();
                    StringBuilder sb = new StringBuilder();

                    while (line != null) {
                        sb.append(line).append("\n");
                        line = buf.readLine();

                        fileAsString = sb.toString();
//                        System.out.println("Contents : " + fileAsString);
                    }
                } catch (IOException var1) {
                    System.out.println("Error read file  : " + var1.getMessage());
                    var1.printStackTrace();
                }


                try {
                    List splitFile = Arrays.asList(fileAsString.split("--"));
                    for (int j = 0; j < splitFile.size(); j++) {
                        String comment = splitFile.get(j).toString();
                        if (comment.toLowerCase().contains("purpose")) {
                            System.out.println("Contents : " + comment);
                            sp.purpose = comment.split(":")[1].trim();
                        }
                        //todo версионность доставать отсюда

                    }
                } catch (NullPointerException var2) {
                    System.out.println("No purpose found in file  : " + var2.getMessage());
                    var2.printStackTrace();
                }

                spInfoMap.put(sqlFiles.get(i).getAbsolutePath(), sp);

            }
        }
    }


    public void analyzeSQL() {
        for (int i = 0; i < files.size(); i++) {
            TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvteradata);
            sqlparser.sqlfilename = files.get(i);
            int ret = sqlparser.parse();
            if (ret != 0) {
                System.out.println("Parse file "
                        + sqlparser.sqlfilename
                        + " failed.");
                System.out.println(sqlparser.getErrormessage());
                continue;
            }
            spInfo sp = (spInfo) spInfoMap.get(files.get(i));
            analyzeSQL(sp, sqlparser);
        }
    }

    protected void analyzeSQL(spInfo spInfo, TGSqlParser sqlparser ) {
        procedureInfo procedureInfo = new procedureInfo();
        spInfo.procedures.add(procedureInfo);
//        System.out.println("sqlparser.sqlstatements.size()" + sqlparser.sqlstatements.size());
        for (int i = 0; i < sqlparser.sqlstatements.size(); i++) {
            TCustomSqlStatement sql = sqlparser.sqlstatements.get(i);
            if (procedureInfo != null) {
                analyzeSqlStatement(procedureInfo, sql, i + 1);
            }
        }
    }

    private void analyzeSqlStatement(procedureInfo procedureInfo,
                                     TCustomSqlStatement stmt, int numberStmt) {
        if (stmt instanceof TBlockSqlStatement) {
            TBlockSqlStatement block = (TBlockSqlStatement) stmt;
            if (block.getBodyStatements() != null) {
                for (int i = 0; i < block.getBodyStatements().size(); i++) {
                    analyzeSqlStatement(procedureInfo,
                            block.getBodyStatements().get(i), numberStmt);
                }
            }
//        } else if (stmt instanceof TCommentOnSqlStmt) {
//
//            System.out.println("Stmt : " + ((TCommentOnSqlStmt) stmt).getMessage());
//        } else if (stmt instanceof TCallStatement) {
//            TCallStatement call = (TCallStatement) stmt;
////            System.out.println("We inside");
//            if (call.getAncestorStmt() != null) {
////                System.out.println("Debug");
//
////                System.out.println(call.getArgs());
//                operateInfo operateInfo = new operateInfo();
////                operateInfo.objectType = objectType.SP;
////                operateInfo.objectUsed = call.getRoutineName()
////                        .toString( )
////                        .trim( );
////                operateInfo.usageType = usageType.Call;
//                TExecParameterList columns = call.getParameterList();
//                for (int i = 0; i < columns.size(); i++) {
//                    TExecParameter column = columns.getExecParameter(i);
//                    operateInfo.columns.add(column.getParameterValue().toString());
//                }
//                procedureInfo.operates.add(operateInfo);
//            }
//        }
//        //TODO исправить if else, ошибки почти гарантированы
//        else if (stmt instanceof TIfStmt) {
//
//            TIfStmt ifElse = (TIfStmt) stmt;
//            if (ifElse.getThenStatements() != null) {
//                analyzeSqlStatement(procedureInfo, ifElse.getAncestorStmt());
//            }
//            if (ifElse.getCondition() != null) {
//
//            }
//
//            if (ifElse.getElseStatements() != null) {
//                analyzeSqlStatement(procedureInfo, ifElse.getTopStatement());
//            }
//        } else if (stmt instanceof TMssqlDeclare) {
//            TMssqlDeclare declareStmt = (TMssqlDeclare) stmt;
//            if (declareStmt.getSubquery() != null
//                    && declareStmt.getSubquery().toString().trim().length() > 0) {
//                analyzeSqlStatement(procedureInfo, declareStmt.getSubquery());
//            }
//        } else if (stmt instanceof TCreateTableSqlStatement) {
//            TCreateTableSqlStatement createStmt = (TCreateTableSqlStatement) stmt;
//            TColumnDefinitionList columns = createStmt.getColumnList();
//            operateInfo operateInfo = new operateInfo();
////            operateInfo.objectType = objectType.Table;
////            operateInfo.objectUsed = createStmt.getTargetTable( )
////                    .toString( )
////                    .trim( );
////            operateInfo.usageType = usageType.Create;
//            for (int i = 0; i < columns.size(); i++) {
//                TColumnDefinition column = columns.getColumn(i);
//                operateInfo.columns.add(column.getColumnName().toString());
//            }
//            procedureInfo.operates.add(operateInfo);
//
//        } else if (stmt instanceof TTeradataLock) {
//
//            TTeradataLock lockTableStmt = (TTeradataLock) stmt;
//            operateInfo operateInfo = new operateInfo();
////            operateInfo.objectType = objectType.Table;
////            operateInfo.objectUsed = lockTableStmt.getObjectName( )
////                    .toString( )
////                    .trim( );
////            operateInfo.usageType = usageType.Lock;
//
//            procedureInfo.operates.add(operateInfo);
//
//            if (lockTableStmt.getSqlRequest() != null) {
//                analyzeSqlStatement(procedureInfo, lockTableStmt.getSqlRequest());
//            }
        } else if (stmt instanceof TInsertSqlStatement) {
            TInsertSqlStatement insertStmt = (TInsertSqlStatement) stmt;
            TObjectNameList columns = insertStmt.getColumnList();
            if(stmt.getParentStmt() == null) {
                stmtNumbers.put(stmt.hashCode(), Integer.toString(numberStmt));
            }
            operateInfo operateInfo = new operateInfo();
            if (insertStmt.getSubQuery() != null) {
//                procedureInfo.operates.add(operateInfo);
                analyzeSqlStatement(procedureInfo, insertStmt.getSubQuery(), numberStmt);
            } else {
                procedureInfo.operates.add(operateInfo);
                operateInfo.tgtSchema = insertStmt.getTargetTable().getPrefixSchema().trim();
                operateInfo.tgtTable = insertStmt.getTargetTable().getName().trim();
                operateInfo.type = usageType.Insert;
                if (columns != null) {
                    for (int i = 0; i < columns.size(); i++) {
                        TObjectName column = columns.getObjectName(i);
                        operateInfo.columns.add(column.toString());
                    }
                }
            }
        }
//        else if ( stmt instanceof TDb2WhileStmt)
//        {
//            TDb2WhileStmt insertStmt = (TDb2WhileStmt) stmt;
//            TExpression columns = insertStmt.getCondition( );
//            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = insertStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Insert;
//
//                    operateInfo.columns.add( columns.toString());
//            procedureInfo.operates.add( operateInfo );
//        }
//        else if (stmt instanceof TUpdateSqlStatement) {
//            TUpdateSqlStatement updateStmt = (TUpdateSqlStatement) stmt;
//            TResultColumnList columns = updateStmt.getResultColumnList();
//            operateInfo operateInfo = new operateInfo();
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = updateStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Update;
//            for (int i = 0; i < columns.size(); i++) {
//                TResultColumn column = columns.getResultColumn(i);
//                operateInfo.columns.add(column.getExpr()
//                        .getLeftOperand()
//                        .toString());
////            }
//            procedureInfo.operates.add(operateInfo);
//        } else if (stmt instanceof TDeleteSqlStatement) {
//            TDeleteSqlStatement deleteStmt = (TDeleteSqlStatement) stmt;
//            operateInfo operateInfo = new operateInfo();
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = deleteStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Delete;
//            procedureInfo.operates.add(operateInfo);
//        } else if (stmt instanceof TMssqlDropTable) {
//            TMssqlDropTable dropStmt = (TMssqlDropTable) stmt;
//            operateInfo operateInfo = new operateInfo();
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = dropStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Drop;
//            procedureInfo.operates.add(operateInfo);
//        } else if (stmt instanceof TDropTableSqlStatement) {
//            TDropTableSqlStatement dropStmt = (TDropTableSqlStatement) stmt;
//            operateInfo operateInfo = new operateInfo();
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = dropStmt.getTableName( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Drop;
//            procedureInfo.operates.add(operateInfo);
//        }
        else if (stmt instanceof TSelectSqlStatement) {
            TSelectSqlStatement selectStmt = (TSelectSqlStatement) stmt;
            List<columnInfo> columnInfos = new ArrayList<columnInfo>();
            List<tableInfo> tableInfos = new ArrayList<tableInfo>();
            String parentNumber = "";
            if (selectStmt.getParentStmt() != null) {
                parentNumber = stmtNumbers.get(selectStmt.getParentStmt().hashCode()).toString();
            }
            tableTokensInStmt(columnInfos, tableInfos, selectStmt, parentNumber, 1);
            Map columnMap = new HashMap();

            for (int i = 0; i < columnInfos.size(); i++) {

                columnInfo column = columnInfos.get(i);
                tableInfo table = column.table;
                if (columnMap.containsKey(table)) {
                    List<columnInfo> columns = (List<columnInfo>) columnMap.get(table);
                    boolean flag = false;
                    for (columnInfo temp : columns) {
                        if (temp.toString()
                                .equalsIgnoreCase(column.toString())) {
                            flag = true;
                            break;
                        }
                    }
                    if (!flag) {
                        columns.add(column);
                    }
                } else {
                    List<columnInfo> columns = new ArrayList<columnInfo>();
                    columnMap.put(table, columns);
                    columns.add(column);
                }
            }

            for (int i = 0; i < tableInfos.size(); i++) {
                operateInfo operateInfo = new operateInfo();
                operateInfo.joinType = tableInfos.get(i).joinType;
                operateInfo.onCondition = tableInfos.get(i).onCondition;
                operateInfo.alias = tableInfos.get(i).alias;
                operateInfo.where = tableInfos.get(i).where;
                if (selectStmt.getParentStmt() != null) {
                    operateInfo.operateInfoNumber = tableInfos.get(i).tableNumber;
                    for(int j = 0; j < selectStmt.getParentStmt().getTargetTable().getLinkedColumns().size(); j++){
//                        pri("j", selectStmt.getParentStmt().getTargetTable().getLinkedColumns().getObjectName(j).toString());
                        operateInfo.tgtcolumns.add(selectStmt.getParentStmt().getTargetTable().getLinkedColumns().getObjectName(j).toString());

                    }
                    operateInfo.tgtSchema = selectStmt.getParentStmt().getTargetTable().getFullName().split("\\.")[0];
                    operateInfo.tgtTable = selectStmt.getParentStmt().getTargetTable().getFullName().split("\\.")[1];
                    operateInfo.typeTarget = selectStmt.getParentStmt().sqlstatementtype.name().replace("sst", "");
                }
                operateInfo.srcSchema = tableInfos.get(i).toString().split("\\.")[0];
                operateInfo.srcTable = tableInfos.get(i).toString().split("\\.")[1];
                operateInfo.type = usageType.Select;
                if (columnMap.containsKey(tableInfos.get(i))) {
                    for (columnInfo column : (List<columnInfo>) columnMap.get(tableInfos.get(i))) {
                        operateInfo.columns.add(column.toString());
//                        operateInfo.objectUsed = column.table.toString( );
                    }
                }

                procedureInfo.operates.add(operateInfo);
            }
        }
    }


    public String getDBObjectRelationsAnalysisResult() {
        if (relationBuffer.length() == 0 && files != null) {
            for (String file : files) {
                spInfo spInfo = (spInfo) spInfoMap.get(file);
                for (procedureInfo procedure : spInfo.procedures) {

                    for (operateInfo info : procedure.operates) {

                        StringBuilder builder = new StringBuilder();
                        for (int i = 0; i < info.columns.size(); i++) {
                            builder.append(info.columns.get(i));
                            if (i < info.columns.size() - 1) {
                                builder.append(",");
                            }
                        }
                        for (int i = 0; i < info.tgtcolumns.size(); i++) {
                            builder.append(info.tgtcolumns.get(i));
                            if (i < info.tgtcolumns.size() - 1) {
                                builder.append(",");
                            }
                        }
                        relationBuffer
                                .append(info.operateInfoNumber == null ? "" : info.operateInfoNumber)
                                .append(delimiter)
                                .append(spInfo.purpose == null ? "" : spInfo.purpose)
                                .append(delimiter)
                                .append(info.tgtSchema == null ? "" : info.tgtSchema)
                                .append(delimiter)
                                .append(info.tgtTable == null ? "" : info.tgtTable)
                                .append(delimiter)
                                .append(info.typeTarget == null ? "" : info.typeTarget)
                                .append(delimiter)
                                .append(info.tgtcolumns)
                                .append(delimiter)
                                .append(info.srcSchema == null ? "" : info.srcSchema)
                                .append(delimiter)
                                .append(info.srcTable == null ? "" : info.srcTable)
                                .append(delimiter)
                                .append(info.alias == null ? "" : info.alias)
                                .append(delimiter)
                                .append(info.type == null ? "" : info.type)
                                .append(delimiter)
                                .append(info.joinType == null ? "" : info.joinType)
                                .append(delimiter)
                                .append(info.onCondition == null ? "" : info.onCondition)
                                .append(delimiter)
                                .append(info.where == null ? "" : info.where)
                                .append(delimiter)
                                .append(info.set == null ? "" : info.set)
                                .append(delimiter)
                                .append(spInfo.file == null ? "" : spInfo.file)
                                .append(delimiter)
                                .append(info.callParams == null ? "" : info.callParams)
                                .append(delimiter)
                                .append(info.columns)
                                .append(delimiter)
                                .append(spInfo.version == null ? "" : spInfo.version)
                                .append("\r\n");
                    }

                }
            }
        }
        return relationBuffer.toString();
    }


    protected void tableTokensInStmt(List<columnInfo> columnInfos,
                                     List<tableInfo> tableInfos, TCustomSqlStatement stmt, String parentNumber, int subNum) {
        stmtNumbers.put(stmt.hashCode(), parentNumber + "_" + subNum);
        for (int i = 0; i < stmt.getStatements().size(); i++) {

            tableTokensInStmt(columnInfos, tableInfos, stmt.getStatements()
                    .get(i), parentNumber, subNum + 1);
        }
        for (int i = 0; i < stmt.tables.size(); i++) {
            if (stmt.tables.getTable(i).isBaseTable()) {
                if ((stmt.dbvendor == EDbVendor.dbvteradata)
                        && ((stmt.tables.getTable(i).getFullName().equalsIgnoreCase("deleted")) || (stmt.tables.getTable(i)
                        .getFullName().equalsIgnoreCase("inserted")))) {
                    continue;
                }

                if (stmt.tables.getTable(i).getEffectType() == ETableEffectType.tetSelectInto) {
                    continue;
                }
                tableInfo tableInfo = new tableInfo();
                tableInfo.fullName = stmt.tables.getTable(i).getFullName();
                tableInfo.alias = stmt.tables.getTable(i).getAliasName();
                tableInfo.tableNumber = Integer.toString(i + 1);
                if (stmt.getParentStmt() != null) {
                    String myParentNumber = stmtNumbers.get(stmt.getParentStmt().hashCode()).toString();
                    tableInfo.tableNumber = myParentNumber + "_" + Integer.toString(i + 1);
                }
                tableInfos.add(tableInfo);
                if (stmt.getWhereClause() != null) {
                    tableInfo.where = stmt.getWhereClause().toScript();
                }

                if (stmt.joins.getJoin(0).getJoinItems().size() != 0 & i <= stmt.joins.getJoin(0).getJoinItems().size()) {
                    for (int j = 0; j < stmt.joins.getJoin(0).getJoinItems().size(); j++) {
                        if (stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getTable().equals(stmt.tables.getTable(i))) {
                            String join = stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getJoinType().toString();
                            tableInfo.onCondition = stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getOnCondition().toScript().replace("\\n", "");
                            switch (join) {
                                case "inner":
                                    tableInfo.joinType = joinType.inner;
                                    break;
                                case "left":
                                    tableInfo.joinType = joinType.left;
                                    break;
                                case "right":
                                    tableInfo.joinType = joinType.right;
                                    break;
                                case "full":
                                    tableInfo.joinType = joinType.full;
                                    break;
                            }
                        }
                    }
                }else{
                    if(stmt.tables.getTable(i).isBaseTable()){
                        tableInfo.joinType = joinType.main;
                    }
                }

                for (int j = 0; j < stmt.tables.getTable(i)
                        .getLinkedColumns()
                        .size(); j++) {

                    columnInfo columnInfo = new columnInfo();
                    columnInfo.table = tableInfo;
                    columnInfo.column = stmt.tables.getTable(i)
                            .getLinkedColumns()
                            .getObjectName(j);
                    columnInfos.add(columnInfo);
                }
            }
        }

        if (stmt instanceof TSelectSqlStatement
                && ((TSelectSqlStatement) stmt).getIntoClause() != null) {
            TExpressionList tables = ((TSelectSqlStatement) stmt).getIntoClause()
                    .getExprList();
            for (int j = 0; j < tables.size(); j++) {
                tableInfo tableInfo = new tableInfo();
                tableInfo.fullName = tables.getExpression(j).toString();
                tableInfo.stmt = stmt;
                tableInfos.add(tableInfo);
            }
        }
    }

    void pri (String k, String v) { System.out.println(k +" = "+ v); }
    void pri (String k, int v) { System.out.println(k +" = "+ Integer.toString(v)); }

}


class columnInfo {

    public tableInfo table;
    public TObjectName column;

    public String toString() {
        return column == null ? "" : column.getColumnNameOnly().trim();
    }
};


class operateInfo {
    public String operateInfoNumber;
    public String tgtSchema;
    public String tgtTable;
    public String typeTarget;
    public String srcSchema;
    public String srcTable;
    public String alias;
    public usageType type;
    public joinType joinType;
    public String onCondition;
    public String where;
    public String set;
    public String callParams;
    //    public String objectUsed;
    public List<String> columns = new ArrayList<String>();
    public List<String> tgtcolumns = new ArrayList<String>();
}

class procedureInfo {

    public List<operateInfo> operates = new ArrayList<operateInfo>();
    public procedureInfo() {}
}
// file info
class spInfo {

    public String file;
    public String purpose;
    public String version;
    public List<procedureInfo> procedures = new ArrayList<procedureInfo>();
}

class tableInfo {

    public String tableNumber;
    public String fullName;
    public joinType joinType;
    public String onCondition;
    public String alias;
    public String where;
    public TCustomSqlStatement stmt;

    public String toString() {
        return (fullName == null ? "" : fullName.trim());
    }
}

enum usageType {
    Select, Insert, Update, Create, Delete, Drop, Call, Lock, Exec, Read
}

enum joinType {
    inner, left, right, full, main
}