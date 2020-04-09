

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.*;
import gudusoft.gsqlparser.stmt.db2.TDb2WhileStmt;
import gudusoft.gsqlparser.stmt.mssql.TMssqlBlock;
import gudusoft.gsqlparser.stmt.mssql.TMssqlCreateProcedure;
import gudusoft.gsqlparser.stmt.mssql.TMssqlDeclare;
import gudusoft.gsqlparser.stmt.mssql.TMssqlDropTable;
import gudusoft.gsqlparser.stmt.mssql.TMssqlExecute;
import gudusoft.gsqlparser.stmt.mssql.TMssqlIfElse;
import gudusoft.gsqlparser.stmt.teradata.TTeradataLock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Analyze_SP
{

    public static void main( String[] args )
    {
        if ( args.length == 0 )
        {
            System.out.println( "Usage: java Analyze_SP scriptfile [/o <output file path>] [/d <csv delimiter character>]" );
            return;
        }
        System.out.println(args.toString());
        List array = Arrays.asList( args );
        List<File> files = new ArrayList<File>( );
        for ( int i = 0; i < array.size( ); i++ )
        {
            File file = new File( array.get( i ).toString( ) );
            if ( file.exists( ) )
                files.add( file );
            else
                break;
        }

        String outputFile = null;
        int index = array.indexOf( "/o" );
        if ( index != -1 && args.length > index + 1 )
        {
            outputFile = args[index + 1];
        }
        String delimiter = "|";
        index = array.indexOf( "/d" );
        if ( index != -1 && args.length > index + 1 )
        {
            delimiter = args[index + 1];
        }

        Analyze_SP impact = new Analyze_SP( files, delimiter );
        impact.analyzeSQL( );

        PrintStream writer = null;
        if ( outputFile != null )
        {
            try
            {
                writer = new PrintStream( new FileOutputStream( outputFile,
                        false ) );
                System.setOut( writer );
            }
            catch ( FileNotFoundException e )
            {
                e.printStackTrace( );
            }

        }

        System.out.println( "Purpose"
                + delimiter
                + "Target schema"
                + delimiter
                + "Target table"
                + delimiter
                + "Src_schema"
                + delimiter
                + "Src_table"
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
                + "Max_cdc_date" );
//            System.out.println( "DB of Anayzed Object"
//                    + delimiter
//                    + "Name of Analyzed Object"
//                    + delimiter
//                    + "Object Type"
//                    + delimiter
//                    + "Object Used"
//                    + delimiter
//                    + "Object Type"
//                    + delimiter
//                    + "Usage Type"
//                    + delimiter
//                    + "Columns" );
            System.out.println( impact.getDBObjectRelationsAnalysisResult( ) );


        if ( writer != null )
        {
            writer.close( );
        }

    }

    private StringBuilder relationBuffer = new StringBuilder( );
    private Map spInfoMap = new HashMap( );
    private List<String> files = new ArrayList<String>( );
    private String delimiter;

    public Analyze_SP( List<File> sqlFiles, String delimiter )
    {
        this.delimiter = delimiter;
        if ( sqlFiles.size( ) > 0 )
        {
            for ( int i = 0; i < sqlFiles.size( ); i++ )
            {
                files.add( sqlFiles.get( i ).getAbsolutePath( ) );
                spInfo sp = new spInfo( );
                sp.file = sqlFiles.get( i ).getName( );
                spInfoMap.put( sqlFiles.get( i ).getAbsolutePath( ), sp );

            }
        }
    }


    public void analyzeSQL( )
    {
        for ( int i = 0; i < files.size( ); i++ )
        {
            TGSqlParser sqlparser = new TGSqlParser( EDbVendor.dbvteradata );
            sqlparser.sqlfilename = files.get( i );
            int ret = sqlparser.parse( );
            if ( ret != 0 )
            {
                System.out.println( "Parse file "
                        + sqlparser.sqlfilename
                        + " failed." );
                System.out.println( sqlparser.getErrormessage( ) );
                continue;
            }
            spInfo sp = (spInfo) spInfoMap.get( files.get( i ) );
            analyzeSQL( sp, sqlparser );
        }
    }

    protected void analyzeSQL( spInfo spInfo, TGSqlParser sqlparser )
    {
        procedureInfo procedureInfo = new procedureInfo( spInfo.file);
        spInfo.procedures.add( procedureInfo );
        for ( int i = 0; i < sqlparser.sqlstatements.size( ); i++ )
        {
            TCustomSqlStatement sql = sqlparser.sqlstatements.get( i );
            if ( procedureInfo != null )
            {
                analyzeSqlStatement( procedureInfo, sql );
            }
        }
    }

    private void analyzeSqlStatement( procedureInfo procedureInfo,
                                      TCustomSqlStatement stmt )
    {
        if ( stmt instanceof TBlockSqlStatement )
        {
            TBlockSqlStatement block = (TBlockSqlStatement) stmt;
            if ( block.getBodyStatements( ) != null )
            {
                for ( int i = 0; i < block.getBodyStatements( ).size( ); i++ )
                {
                    analyzeSqlStatement( procedureInfo,
                            block.getBodyStatements( ).get( i ) );
                }
            }
        }
        else if (stmt instanceof TCallStatement){
            TCallStatement call = (TCallStatement) stmt;
            System.out.println("We inside");
            if ( call.getAncestorStmt( ) != null )
            {
                System.out.println("Debug");

                System.out.println(call.getArgs());
                operateInfo operateInfo = new operateInfo( );
//                operateInfo.objectType = objectType.SP;
//                operateInfo.objectUsed = call.getRoutineName()
//                        .toString( )
//                        .trim( );
//                operateInfo.usageType = usageType.Call;
                TExecParameterList columns = call.getParameterList();
                for ( int i = 0; i < columns.size( ); i++ )
                {
                    TExecParameter column = columns.getExecParameter( i );
                    operateInfo.columns.add( column.getParameterValue( ).toString( ) );
                }
                procedureInfo.operates.add( operateInfo );
            }
        }
        //TODO исправить if else, ошибки почти гарантированы
        else if ( stmt instanceof TIfStmt )
        {

            TIfStmt ifElse = (TIfStmt) stmt;
            if ( ifElse.getThenStatements( ) != null )
            {
                analyzeSqlStatement( procedureInfo, ifElse.getAncestorStmt( ) );
            }
            if ( ifElse.getCondition( ) != null )
            {

            }

            if ( ifElse.getElseStatements( ) != null )
            {
                analyzeSqlStatement( procedureInfo, ifElse.getTopStatement() );
            }
        }
        else if ( stmt instanceof TMssqlDeclare )
        {
            TMssqlDeclare declareStmt = (TMssqlDeclare) stmt;
            if ( declareStmt.getSubquery( ) != null
                    && declareStmt.getSubquery( ).toString( ).trim( ).length( ) > 0 )
            {
                analyzeSqlStatement( procedureInfo, declareStmt.getSubquery( ) );
            }
        }
        else if ( stmt instanceof TCreateTableSqlStatement )
        {
            TCreateTableSqlStatement createStmt = (TCreateTableSqlStatement) stmt;
            TColumnDefinitionList columns = createStmt.getColumnList( );
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = createStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Create;
            for ( int i = 0; i < columns.size( ); i++ )
            {
                TColumnDefinition column = columns.getColumn( i );
                operateInfo.columns.add( column.getColumnName( ).toString( ) );
            }
            procedureInfo.operates.add( operateInfo );

        }
        else if ( stmt instanceof TTeradataLock)
        {

            TTeradataLock lockTableStmt = (TTeradataLock) stmt;
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = lockTableStmt.getObjectName( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Lock;

            procedureInfo.operates.add( operateInfo );

            if (lockTableStmt.getSqlRequest() != null)
            {
                analyzeSqlStatement(procedureInfo, lockTableStmt.getSqlRequest());
            }
        }
        else if ( stmt instanceof TInsertSqlStatement )
        {
            TInsertSqlStatement insertStmt = (TInsertSqlStatement) stmt;
            TObjectNameList columns = insertStmt.getColumnList( );
            operateInfo operateInfo = new operateInfo( );
            operateInfo.tgtSchema = insertStmt.getTargetTable().getPrefixSchema().trim( );
            operateInfo.tgtTable = insertStmt.getTargetTable().getName().trim( );
            operateInfo.type = usageType.Insert;
            if ( columns != null )
            {
                for ( int i = 0; i < columns.size( ); i++ )
                {
                    TObjectName column = columns.getObjectName( i );
                    operateInfo.columns.add( column.toString( ) );
                }
            }
            procedureInfo.operates.add( operateInfo );

             if (insertStmt.getSubQuery() != null)
             {
             analyzeSqlStatement(procedureInfo, insertStmt.getSubQuery());
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
        else if ( stmt instanceof TUpdateSqlStatement )
        {
            TUpdateSqlStatement updateStmt = (TUpdateSqlStatement) stmt;
            TResultColumnList columns = updateStmt.getResultColumnList( );
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = updateStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Update;
            for ( int i = 0; i < columns.size( ); i++ )
            {
                TResultColumn column = columns.getResultColumn( i );
                operateInfo.columns.add( column.getExpr( )
                        .getLeftOperand( )
                        .toString( ) );
            }
            procedureInfo.operates.add( operateInfo );
        }
        else if ( stmt instanceof TDeleteSqlStatement )
        {
            TDeleteSqlStatement deleteStmt = (TDeleteSqlStatement) stmt;
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = deleteStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Delete;
            procedureInfo.operates.add( operateInfo );
        }
        else if ( stmt instanceof TMssqlDropTable )
        {
            TMssqlDropTable dropStmt = (TMssqlDropTable) stmt;
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = dropStmt.getTargetTable( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Drop;
            procedureInfo.operates.add( operateInfo );
        }
        else if ( stmt instanceof TDropTableSqlStatement )
        {
            TDropTableSqlStatement dropStmt = (TDropTableSqlStatement) stmt;
            operateInfo operateInfo = new operateInfo( );
//            operateInfo.objectType = objectType.Table;
//            operateInfo.objectUsed = dropStmt.getTableName( )
//                    .toString( )
//                    .trim( );
//            operateInfo.usageType = usageType.Drop;
            procedureInfo.operates.add( operateInfo );
        }
        else if ( stmt instanceof TSelectSqlStatement )
        {
            TSelectSqlStatement selectStmt = (TSelectSqlStatement) stmt;
            List<columnInfo> columnInfos = new ArrayList<columnInfo>( );
            List<tableInfo> tableInfos = new ArrayList<tableInfo>( );
            tableTokensInStmt( columnInfos, tableInfos, selectStmt );
            Map columnMap = new HashMap( );

            for ( int i = 0; i < columnInfos.size( ); i++ )
            {

                columnInfo column = columnInfos.get( i );
                tableInfo table = column.table;
                if ( columnMap.containsKey( table ) )
                {
                    List<columnInfo> columns = (List<columnInfo>) columnMap.get( table );
                    boolean flag = false;
                    for ( columnInfo temp : columns )
                    {
                        if ( temp.toString( )
                                .equalsIgnoreCase( column.toString( ) ) )
                        {
                            flag = true;
                            break;
                        }
                    }
                    if ( !flag )
                    {
                        columns.add( column );
                    }
                }
                else
                {
                    List<columnInfo> columns = new ArrayList<columnInfo>( );
                    columnMap.put( table, columns );
                    columns.add( column );
                }
            }

            for ( int i = 0; i < tableInfos.size( ); i++ )
            {


                operateInfo operateInfo = new operateInfo( );
                operateInfo.joinType = tableInfos.get(i).joinType;
                operateInfo.onCondition = tableInfos.get(i).onCondition;
                if ( tableInfos.get( i ).stmt instanceof TSelectSqlStatement
                        && ( (TSelectSqlStatement) tableInfos.get( i ).stmt ).getIntoClause( ) != null ) {
                    operateInfo.type = usageType.Insert;
                    operateInfo.tgtSchema = tableInfos.get( i ).toString().split("\\.")[0];
                    operateInfo.tgtTable = tableInfos.get( i ).toString( ).split("\\.")[1];
                }

            else {
                    operateInfo.type = usageType.Select;
                    operateInfo.srcSchema = tableInfos.get( i ).toString().split("\\.")[0];
                    operateInfo.srcTable = tableInfos.get( i ).toString( ).split("\\.")[1];
                }
                if ( columnMap.containsKey( tableInfos.get( i ) ) )
                {
                    for ( columnInfo column : (List<columnInfo>) columnMap.get( tableInfos.get( i ) ) )
                    {
                        operateInfo.columns.add( column.toString( ) );
//                        operateInfo.objectUsed = column.table.toString( );
                    }
                }
                if( selectStmt.getWhereClause() != null)
                {
//                    System.out.println("Where  " + selectStmt.getWhereClause());
                    operateInfo.where = selectStmt.getWhereClause().toScript();
                }


                if( selectStmt.getJoins() != null)
                {
//                    System.out.println("Where  " + selectStmt.getJoins());
//                    operateInfo.where = selectStmt.getWhereClause().toScript();
                }

                procedureInfo.operates.add( operateInfo );
            }
        }
    }


    public String getDBObjectRelationsAnalysisResult( )
    {
        if ( relationBuffer.length( ) == 0 && files != null )
        {
            for ( String file : files )
            {
                spInfo spInfo = (spInfo) spInfoMap.get( file );
                for ( procedureInfo procedure : spInfo.procedures )
                {
                    for ( operateInfo info : procedure.operates )
                    {
                        StringBuilder builder = new StringBuilder( );
                        for ( int i = 0; i < info.columns.size( ); i++ )
                        {
                            builder.append( info.columns.get( i ) );
                            if ( i < info.columns.size( ) - 1 )
                            {
                                builder.append( "," );
                            }
                        }
//                        System.out.println( "Purpose"
//                                + delimiter
//                                + "Target schema"
//                                + delimiter
//                                + "Target table"
//                                + delimiter
//                                + "Src_schema"
//                                + delimiter
//                                + "Src_table"
//                                + delimiter
//                                + "Type"
//                                + delimiter
//                                + "Join type"
//                                + delimiter
//                                + "Where"
//                                + delimiter
//                                + "Set"
//                                + delimiter
//                                + "Src file name"
//                                + delimiter
//                                + "Call Params"
//                                + delimiter
//                                + "Columns"
//                                + delimiter
//                                + "Max_cdc_date" );

                        relationBuffer
                                .append(spInfo.purpose == null ? "" : spInfo.purpose)
//                                .append(spInfo.db == null ? ""
//                                : spInfo.db )
                                .append( delimiter )
                                .append( info.tgtSchema == null ? "" : info.tgtSchema)
                                .append( delimiter )
                                .append( info.tgtTable == null ? "" : info.tgtTable)
                                .append( delimiter )
                                .append( info.srcSchema == null ? "" : info.srcSchema)
                                .append( delimiter )
                                .append( info.srcTable == null ? "" : info.srcTable)
                                .append( delimiter )
                                .append( info.type == null ? "" : info.type)
                                .append( delimiter )
                                .append( info.joinType == null ? "" : info.joinType)
                                .append( delimiter )
                                .append( info.onCondition == null ? "" : info.onCondition)
                                .append( delimiter )
                                .append( info.where == null ? "" : info.where)
                                .append( delimiter )
                                .append( info.set == null ? "" : info.set)
                                .append( delimiter )
                                .append( spInfo.file == null ? "" : spInfo.file)
                                .append( delimiter )
                                .append( info.callParams == null ? "" : info.callParams)
                                .append( delimiter )
                                .append( info.columns )
                                .append( delimiter )
                                .append( spInfo.version == null ? "" : spInfo.version)
                                .append( "\r\n" );
                    }

                }
            }
        }
        return relationBuffer.toString( );
    }


    protected void tableTokensInStmt( List<columnInfo> columnInfos,
                                      List<tableInfo> tableInfos, TCustomSqlStatement stmt )
    {
        for ( int i = 0; i < stmt.getStatements( ).size( ); i++ )
        {
            tableTokensInStmt( columnInfos, tableInfos, stmt.getStatements( )
                    .get( i ) );
        }
        for ( int i = 0; i < stmt.tables.size( ); i++ )
        {
            if ( stmt.tables.getTable( i ).isBaseTable( ) )
            {
                if ( ( stmt.dbvendor == EDbVendor.dbvteradata )
                        && ( ( stmt.tables.getTable( i ).getFullName( ).equalsIgnoreCase( "deleted" ) ) || ( stmt.tables.getTable( i )
                        .getFullName( ).equalsIgnoreCase( "inserted" ) ) ) )
                {
                    continue;
                }

                if ( stmt.tables.getTable( i ).getEffectType( ) == ETableEffectType.tetSelectInto )
                {
                    continue;
                }
                tableInfo tableInfo = new tableInfo( );
                tableInfo.fullName = stmt.tables.getTable( i ).getFullName( );
                tableInfos.add( tableInfo );
                System.out.println("table info full name : " + tableInfo.fullName);
                if(stmt.joins.getJoin(0).getJoinItems().size() != 0 & i <= stmt.joins.getJoin(0).getJoinItems().size()  ) {

                    for(int j = 0; j < stmt.joins.getJoin(0).getJoinItems().size(); j++){
                        if(stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getTable().equals(stmt.tables.getTable(i))){
//                            System.out.println("Joins " + stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getJoinType());
                            String join = stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getJoinType().toString();
                            System.out.println("OnCondition " + stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getOnCondition().toString());
                            tableInfo.onCondition = stmt.joins.getJoin(0).getJoinItems().getJoinItem(j).getOnCondition().toScript().replace("\\n", "");
                            switch (join) {
                                case "inner" :
                                    tableInfo.joinType = joinType.inner;
                                    break;
                                case "left" :
                                    tableInfo.joinType = joinType.left;
                                    break;
                                case "right" :
                                    tableInfo.joinType = joinType.right;
                                    break;
                                case "full" :
                                    tableInfo.joinType = joinType.full;
                                    break;
                            }
                        }
                    }
                }

                for ( int j = 0; j < stmt.tables.getTable( i )
                        .getLinkedColumns( )
                        .size( ); j++ )
                {

                    columnInfo columnInfo = new columnInfo( );
                    columnInfo.table = tableInfo;
                    columnInfo.column = stmt.tables.getTable( i )
                            .getLinkedColumns( )
                            .getObjectName( j );
                    columnInfos.add( columnInfo );
                }
            }
        }

        if ( stmt instanceof TSelectSqlStatement
                && ( (TSelectSqlStatement) stmt ).getIntoClause( ) != null )
        {
            TExpressionList tables = ( (TSelectSqlStatement) stmt ).getIntoClause( )
                    .getExprList( );
            for ( int j = 0; j < tables.size( ); j++ )
            {
                tableInfo tableInfo = new tableInfo( );
                tableInfo.fullName = tables.getExpression( j ).toString( );
                tableInfo.stmt = stmt;
                tableInfos.add( tableInfo );
            }
        }
    }

}


class columnInfo
{

    public tableInfo table;
    public TObjectName column;

    public String toString( )
    {
        return column == null ? "" : column.getColumnNameOnly( ).trim( );
    }
};


class operateInfo
{
    public String tgtSchema;
    public String tgtTable;
    public String srcSchema;
    public String srcTable;
    public usageType type;
    public joinType joinType;
    public String onCondition;
    public String where;
    public String set;
    public String callParams;
//    public String objectUsed;
    public List<String> columns = new ArrayList<String>( );
}

class procedureInfo
{


    public String name;
//    public objectType objectType;
    public List<operateInfo> operates = new ArrayList<operateInfo>( );
    public boolean hasTryCatch;

    public procedureInfo( String name )
    {
        this.name = name;
    }
}

class spInfo
{

    public String file;
    public String purpose;
    public String version;
    public List<procedureInfo> procedures = new ArrayList<procedureInfo>( );
}

class tableInfo
{

    public String fullName;
    public joinType joinType;
    public String onCondition;
    public TCustomSqlStatement stmt;

    public String toString( )
    {
        return ( fullName == null ? "" : fullName.trim( ) );
    }
}

enum usageType {
    Select, Insert, Update, Create, Delete, Drop, Call, Lock, Exec, Read
}

enum joinType {
    inner, left, right, full
}