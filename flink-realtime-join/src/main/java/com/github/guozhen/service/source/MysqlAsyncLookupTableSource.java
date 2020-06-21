package com.github.guozhen.service.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;


public class MysqlAsyncLookupTableSource implements LookupableTableSource<Row> {
    private final String tableName;
    private final String[] fieldNames;
    private final String[] connectionField;
    private final TypeInformation[] fieldTypes;

    private MysqlAsyncLookupTableSource(String tableName,String[] fieldNames, String[] connectionField, TypeInformation[] fieldTypes) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.connectionField = connectionField;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    //  使用AsyncTableFunction，加载维表数据
    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return MysqlAsyncLookupFunction.Builder.getBuilder()
                .withTableNames(tableName)
                .withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .withConnectionField(connectionField)
                .build();
    }

    @Override
    public boolean isAsyncEnabled() {
        return true;
    }
    // 读取的数据类型
    @Override
    public DataType getProducedDataType() {
        // 旧版本的Typeinfo类型转新版本的DataType
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    public static final class Builder {
        private String tableName;
        private String[] fieldNames;
        private String[] connectionField;
        private TypeInformation[] fieldTypes;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withTableNames(String tableName) {
            this.tableName = tableName;
            return this;
        }
        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withConnectionField(String[] connectionField) {
            this.connectionField = connectionField;
            return this;
        }

        public MysqlAsyncLookupTableSource build() {
            return new MysqlAsyncLookupTableSource(tableName,fieldNames,connectionField, fieldTypes);
        }
    }
}