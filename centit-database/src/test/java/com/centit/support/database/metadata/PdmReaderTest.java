package com.centit.support.database.metadata;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PdmReaderTest {
    private static final String MODEL = """
        <Root xmlns:a="attribute" xmlns:c="collection" xmlns:o="object">
          <c:Tables>
            <o:Table Id="parent-table">
              <a:Code>ParentTable</a:Code><a:Name>Parent label</a:Name><a:Comment>Parent comment</a:Comment>
              <c:Columns>
                <o:Column Id="parent-first"><a:Code>FIRST</a:Code><a:DataType>bigint</a:DataType></o:Column>
                <o:Column Id="parent-second"><a:Code>SECOND</a:Code><a:DataType>bigint</a:DataType></o:Column>
              </c:Columns>
              <c:PrimaryKey><o:Key Ref="parent-key"/></c:PrimaryKey>
              <c:Keys><o:Key Id="parent-key"><a:Code>PK_PARENT</a:Code><c:Key.Columns>
                <o:Column Ref="parent-second"/><o:Column Ref="parent-first"/>
              </c:Key.Columns></o:Key></c:Keys>
            </o:Table>
            <o:Table Id="child-table"><a:Code>ChildTable</a:Code><c:Columns>
              <o:Column Id="child-first"><a:Code>CHILD_FIRST</a:Code><a:DataType>bigint</a:DataType></o:Column>
              <o:Column Id="child-second"><a:Code>CHILD_SECOND</a:Code><a:DataType>bigint</a:DataType></o:Column>
            </c:Columns></o:Table>
          </c:Tables>
          <c:References><o:Reference Id="reference-xml-id"><a:Code>FK_PARENT_CHILD</a:Code>
            <c:ParentKey><o:Key Ref="parent-key"/></c:ParentKey>
            <c:ChildTable><o:Table Ref="child-table"/></c:ChildTable>
            <c:Joins>
              <o:ReferenceJoin><c:Object1><o:Column Ref="parent-first"/></c:Object1><c:Object2><o:Column Ref="child-first"/></c:Object2></o:ReferenceJoin>
              <o:ReferenceJoin><c:Object1><o:Column Ref="parent-second"/></c:Object1><c:Object2><o:Column Ref="child-second"/></c:Object2></o:ReferenceJoin>
            </c:Joins>
          </o:Reference></c:References>
        </Root>
        """;

    private PdmReader read(String xml) {
        PdmReader reader = new PdmReader();
        assertTrue(reader.loadPdmFile(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
        return reader;
    }

    @Test
    void usesRealConstraintAndColumnCodesAndKeepsPrimaryKeyOrder() {
        PdmReader reader = read(MODEL);
        reader.setDBSchema("mixed_schema");
        SimpleTableInfo table = reader.getTableMetadata("ParentTable");
        assertEquals("PARENTTABLE", table.getTableName());
        assertEquals("MIXED_SCHEMA", table.getSchema());
        assertEquals("Parent comment", table.getTableComment());
        assertEquals(List.of("SECOND", "FIRST"), table.getPkFields().stream().map(TableField::getColumnName).toList());
        SimpleTableReference reference = table.getReferences().get(0);
        assertEquals("FK_PARENT_CHILD", reference.getReferenceCode());
        assertEquals("ChildTable", reference.getTableName());
        assertEquals(Map.of("FIRST", "CHILD_FIRST", "SECOND", "CHILD_SECOND"), reference.getReferenceColumns());
        assertNull(table.findReference("FK_PARENT_CHILD"));
        assertNull(table.findReference("UNKNOWN"));
        assertNull(table.findReference(null));
        assertEquals("PARENTTABLE", reader.listTables(false, new String[]{"ParentTable"}).get(0).getTableName());
        assertNull(reader.getTableMetadata("missing"));
    }

    @Test
    void retainsLimitedSupportForMissingReferencedColumns() {
        PdmReader missingColumn = read(MODEL.replace("Ref=\"child-first\"", "Ref=\"missing-column\""));
        assertEquals(Map.of("SECOND", "CHILD_SECOND"),
            missingColumn.getTableMetadata("ParentTable").getReferences().get(0).getReferenceColumns());
    }

    @Test
    void failedLoadClearsPreviousDocumentAndPreservesBooleanResult() {
        PdmReader reader = new PdmReader();
        assertTrue(reader.getAllTableCode().isEmpty());
        assertTrue(reader.listTables(false, null).isEmpty());
        assertNull(reader.getTableMetadata("ParentTable"));
        reader.loadPdmFile(new ByteArrayInputStream(MODEL.getBytes(StandardCharsets.UTF_8)));
        assertFalse(reader.loadPdmFile(
            new ByteArrayInputStream("<invalid".getBytes(StandardCharsets.UTF_8))));
        assertNull(reader.getTableMetadata("ParentTable"));
    }
}
