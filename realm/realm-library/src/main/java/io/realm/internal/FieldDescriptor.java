/*
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.realm.internal;

import java.util.Arrays;
import java.util.List;

import io.realm.RealmFieldType;


/**
 * Class describing a single field possible several links away.
 * TODO: The methods getColumnIndice and sparseFieldDescription from RealmSchema should be integrated here
 */
public abstract class FieldDescriptor {
    public static FieldDescriptor createLegacyDescriptor(Table table, String fieldDescription, boolean allowLink, boolean allowList) {
        return new LegacyFieldDescriptor(table, fieldDescription, allowLink, allowList);
    }

    public static FieldDescriptor createFromCache(String className, String fieldDescription, RealmFieldType[] validColumnTypes) {
        return new CachedFieldDescriptor();
    }

    public static FieldDescriptor createDynamic(Table table, String fieldDescription, RealmFieldType[] validColumnTypes) {
        return new DynamicFieldDescriptor();
    }

    public abstract long[] getColumnIndices();

    public abstract RealmFieldType getFieldType();

    public abstract String getFieldName();

    public abstract boolean hasSearchIndex();

    ColumnInfo getColumnInfo(String currentTable) { return null; }

    long getNativeTablePtr(String currentTable) { return 0L; }

    void verifyColumnType(String tableName, String columnName, RealmFieldType columnType, RealmFieldType... validColumnTypes) {
        if ((validColumnTypes == null) || (validColumnTypes.length <= 0)) {
            return;
        }

        for (int i = 0; i < validColumnTypes.length; i++) {
            if (validColumnTypes[i] == columnType) {
                return;
            }
        }

        throw new IllegalArgumentException(String.format(
                "Invalid query: field '%s' in table '%s' is of invalid type '%s'.",
                columnName, tableName, columnType.toString()));
    }

    /**
     * Parse the passed field description into its components.
     * This must be standard across implementations and is, therefore, implemented in the base class.
     * TODO: This method should be integrated with the class FieldDescriptor.
     *
     * @param fieldDescription a field description.
     * @return the parse tree: a list of column names
     */
    final List<String> parseFieldDescription(String fieldDescription) {
        if (fieldDescription == null || fieldDescription.equals("")) {
            throw new IllegalArgumentException("Invalid query: field name is empty");
        }
        if (fieldDescription.endsWith(".")) {
            throw new IllegalArgumentException("Invalid query: field name must not end with a period ('.')");
        }
        return Arrays.asList(fieldDescription.split("\\."));
    }

    private static class LegacyFieldDescriptor extends FieldDescriptor {
        private long[] columnIndices;
        private RealmFieldType fieldType;
        private String fieldName;
        private boolean searchIndex;


        private LegacyFieldDescriptor(Table table, String fieldDescription, boolean allowLink, boolean allowList) {
            if (fieldDescription == null || fieldDescription.isEmpty()) {
                throw new IllegalArgumentException("Non-empty field name must be provided");
            }
            if (fieldDescription.startsWith(".") || fieldDescription.endsWith(".")) {
                throw new IllegalArgumentException("Illegal field name. It cannot start or end with a '.': " + fieldDescription);
            }
            if (fieldDescription.contains(".")) {
                // Resolves field description down to last field name
                String[] names = fieldDescription.split("\\.");
                long[] columnIndices = new long[names.length];
                for (int i = 0; i < names.length - 1; i++) {
                    long index = table.getColumnIndex(names[i]);
                    if (index == Table.NO_MATCH) {
                        throw new IllegalArgumentException(
                                String.format("Invalid field name: '%s' does not refer to a class.", names[i]));
                    }
                    RealmFieldType type = table.getColumnType(index);
                    if (!allowLink && type == RealmFieldType.OBJECT) {
                        throw new IllegalArgumentException(
                                String.format("'RealmObject' field '%s' is not a supported link field here.", names[i]));
                    } else if (!allowList && type == RealmFieldType.LIST) {
                        throw new IllegalArgumentException(
                                String.format("'RealmList' field '%s' is not a supported link field here.", names[i]));
                    } else if (type == RealmFieldType.OBJECT || type == RealmFieldType.LIST) {
                        table = table.getLinkTarget(index);
                        columnIndices[i] = index;
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Invalid field name: '%s' does not refer to a class.", names[i]));
                    }
                }

                // Check if last field name is a valid field
                String columnName = names[names.length - 1];
                long columnIndex = table.getColumnIndex(columnName);
                columnIndices[names.length - 1] = columnIndex;
                if (columnIndex == Table.NO_MATCH) {
                    throw new IllegalArgumentException(
                            String.format("'%s' is not a field name in class '%s'.", columnName, table.getName()));
                }

                this.fieldType = table.getColumnType(columnIndex);
                this.fieldName = columnName;
                this.columnIndices = columnIndices;
                this.searchIndex = table.hasSearchIndex(columnIndex);
            } else {
                long fieldIndex = table.getColumnIndex(fieldDescription);
                if (fieldIndex == Table.NO_MATCH) {
                    throw new IllegalArgumentException(String.format("Field '%s' does not exist.", fieldDescription));
                }
                this.fieldType = table.getColumnType(fieldIndex);
                this.fieldName = fieldDescription;
                this.columnIndices = new long[] {fieldIndex};
                this.searchIndex = table.hasSearchIndex(fieldIndex);
            }
        }

        @Override
        public long[] getColumnIndices() {
            return Arrays.copyOf(columnIndices, columnIndices.length);
        }

        @Override
        public RealmFieldType getFieldType() {
            return fieldType;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public boolean hasSearchIndex() {
            return searchIndex;
        }
    }

    private static class CachedFieldDescriptor extends FieldDescriptor {

        /**
         * Parses the passed field description (@see parseFieldDescription(String) and returns the information
         * necessary for RealmQuery predicates to select the specified records.
         * Because the values returned by this method will, immediately, be handed to native code, they are
         * in coordinated arrays, not a List&lt;ColumnDeatils&gt;
         * There are two kinds of records.  If return[1][i] is NativeObject.NULLPTR, return[0][i] contains
         * the column index for the i-th element in the dotted field description path.
         * If return[1][i] is *not* NativeObject.NULLPTR, it is a pointer to the source table for a backlink
         * and return[0][i] is the column index of the source column in that table.
         * TODO: This method should be integrated with the class FieldDescriptor.
         *
         * @param tableName the starting Table: where(Table.class)
         * @param fieldDescription fieldName or link path to a field name.
         * @param validColumnTypes valid field type for the last field in a linked field
         * @return a pair of arrays:  [0] is column indices, [1] is either NativeObject.NULLPTR or a native table pointer.
         */
        private long[][] getColumnIndicesCached(String tableName, String fieldDescription, RealmFieldType... validColumnTypes) {
            List<String> fields = parseFieldDescription(fieldDescription);
            int nFields = fields.size();
            if (nFields <= 0) {
                throw new IllegalArgumentException("Invalid query: Empty field descriptor");
            }

            long[][] columnInfo = new long[2][];
            columnInfo[0] = new long[nFields];
            columnInfo[1] = new long[nFields];

            String currentTable = tableName;

            ColumnInfo tableInfo;
            String columnName = null;
            RealmFieldType columnType = null;
            long columnIndex;
            for (int i = 0; i < nFields; i++) {
                columnName = fields.get(i);
                if ((columnName == null) || (columnName.length() <= 0)) {
                    throw new IllegalArgumentException(
                            "Invalid query: Field descriptor contains an empty field.  A field description may not begin with or contain adjacent periods ('.').");
                }

                tableInfo = getColumnInfo(currentTable);
                if (tableInfo == null) {
                    throw new IllegalArgumentException(
                            String.format("Invalid query: table '%s' not found in this schema.", currentTable));
                }

                columnIndex = tableInfo.getColumnIndex(columnName);
                if (columnIndex < 0) {
                    throw new IllegalArgumentException(
                            String.format("Invalid query: field '%s' not found in table '%s'.", columnName, currentTable));
                }

                columnType = tableInfo.getColumnType(columnName);
                // all but the last field must be a link type
                if (i < nFields - 1) {
                    verifyColumnType(currentTable, columnName, columnType, RealmFieldType.OBJECT, RealmFieldType.LIST, RealmFieldType.LINKING_OBJECTS);
                    currentTable = tableInfo.getLinkedTable(columnName);
                }
                columnInfo[0][i] = columnIndex;
                columnInfo[1][i] = (columnType != RealmFieldType.LINKING_OBJECTS)
                        ? NativeObject.NULLPTR
                        : getNativeTablePtr(currentTable);
            }

            verifyColumnType(tableName, columnName, columnType, validColumnTypes);

            return columnInfo;
        }

        @Override
        public long[] getColumnIndices() {
            return new long[0];
        }

        @Override
        public RealmFieldType getFieldType() {
            return null;
        }

        @Override
        public String getFieldName() {
            return null;
        }

        @Override
        public boolean hasSearchIndex() {
            return false;
        }
    }

    private static class DynamicFieldDescriptor extends FieldDescriptor {

        // Backlinks are not supported here.
        private long[][] getColumnIndicesDynamic(Table table, String fieldDescription, RealmFieldType... validColumnTypes) {
            List<String> fields = parseFieldDescription(fieldDescription);
            int nFields = fields.size();
            if (nFields <= 0) {
                throw new IllegalArgumentException("Invalid query: Empty field descriptor");
            }

            long[][] columnInfo = new long[2][];
            columnInfo[0] = new long[nFields];
            columnInfo[1] = new long[nFields];

            Table currentTable = table;

            String tableName = null;
            String columnName = null;
            RealmFieldType columnType = null;
            long columnIndex;
            for (int i = 0; i < nFields; i++) {
                columnName = fields.get(i);
                if ((columnName == null) || (columnName.length() <= 0)) {
                    throw new IllegalArgumentException(
                            "Invalid query: Field descriptor contains an empty field.  A field description may not begin with or contain adjacent periods ('.').");
                }
                // "Invalid query:  field descriptor '" + fieldDescription + "': "

                tableName = currentTable.getClassName();

                columnIndex = currentTable.getColumnIndex(columnName);
                if (columnIndex < 0) {
                    throw new IllegalArgumentException(
                            String.format("Invalid query: field '%s' not found in table '%s'.", columnName, tableName));
                }

                columnType = currentTable.getColumnType(columnIndex);
                // all but the last field must be a link type
                if (i < nFields - 1) {
                    verifyColumnType(tableName, columnName, columnType, RealmFieldType.OBJECT, RealmFieldType.LIST);
                    currentTable = currentTable.getLinkTarget(columnIndex);
                }

                columnInfo[0][i] = columnIndex;
                columnInfo[1][i] = NativeObject.NULLPTR;
            }

            verifyColumnType(tableName, columnName, columnType, validColumnTypes);

            return columnInfo;
        }

        @Override
        public long[] getColumnIndices() {
            return new long[0];
        }

        @Override
        public RealmFieldType getFieldType() {
            return null;
        }

        @Override
        public String getFieldName() {
            return null;
        }

        @Override
        public boolean hasSearchIndex() {
            return false;
        }
    }
}
