package de.rewe.kafkatogcs.schema

import net.jimblackler.jsonschemafriend.Schema
import net.jimblackler.jsonschemafriend.SchemaStore
import org.apache.flink.table.types.DataType
import spock.lang.Specification
import spock.lang.Unroll

import static org.apache.flink.table.api.DataTypes.ARRAY
import static org.apache.flink.table.api.DataTypes.BOOLEAN
import static org.apache.flink.table.api.DataTypes.DOUBLE
import static org.apache.flink.table.api.DataTypes.INT
import static org.apache.flink.table.api.DataTypes.NULL
import static org.apache.flink.table.api.DataTypes.ROW
import static org.apache.flink.table.api.DataTypes.STRING


class ConverterSpec extends Specification {
    def schemaStore = new SchemaStore()

    @Unroll
    def 'Test simple type conversion'() {
        given:
//        Schema inputSchema = schemaStore.loadSchema(ConverterSpec.getResource('/market_v1.json'))
        Schema inputSchema = schemaStore.loadSchema([type: inputType])

        when:
        def dataType = ConvertersKt.convertSchemaToFlinkType(inputSchema)

        then:
        dataType == outputType

        where:
        inputType | outputType
        'null'    | NULL()
        'boolean' | BOOLEAN()
        'string'  | STRING()
        'integer' | INT()
        'number'  | DOUBLE()
    }

    @Unroll
    def 'Test array types'() {
        given:
        Schema inputSchema = schemaStore.loadSchema(array)

        when:
        def dataType = ConvertersKt.convertSchemaToFlinkType(inputSchema)

        then:
        dataType == outputType

        where:
        array                                                             | outputType
        [type: 'array']                                                   | ARRAY(STRING() as DataType)
        [type: 'array', items: [type: 'string']]                          | ARRAY(STRING() as DataType)
        [type: 'array', items: [type: 'integer']]                         | ARRAY(INT() as DataType)
        [type: 'array', items: [type: 'number']]                          | ARRAY(DOUBLE() as DataType)
        [type: 'array', items: [type: 'array', items: [type: 'integer']]] | ARRAY(ARRAY(INT() as DataType) as DataType)
    }

    def 'Test real schema'() {
        given:
        Schema inputSchema = schemaStore.loadSchema(ConverterSpec.getResource('/market_v1.json'))

        when:
        def dataType = ConvertersKt.convertSchemaToFlinkType(inputSchema)

        then:
        dataType.children.size() == 5
    }
}
