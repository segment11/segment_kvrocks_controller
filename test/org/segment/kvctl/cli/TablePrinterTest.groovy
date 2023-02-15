package org.segment.kvctl.cli

import spock.lang.Specification

class TablePrinterTest extends Specification {
    def "Print"() {
        given:
        List<List<String>> table = []
        and:
        10.times {
            List<String> row = []
            10.times {
                def random = new Random()
                row << 'a' * random.nextInt(50)
            }
            table << row
        }
        TablePrinter.print(table)
        expect:
        table.size() == 10
    }
}
