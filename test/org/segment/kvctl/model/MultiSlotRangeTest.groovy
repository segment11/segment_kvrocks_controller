package org.segment.kvctl.model

import spock.lang.Specification

class MultiSlotRangeTest extends Specification {
    def "TotalNumber"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(100, 200)
        def m2 = m.clone()
        m2.addSinge(0, 10)
        println m
        println m2

        expect:
        m.totalNumber() == 101
        m2.totalNumber() == 101 + 11
        // sorted
        m2.toString() == '0-10,100-200'
    }

    def "AddSinge"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        def m2 = m.clone()
        m2.addSinge(100, 200)
        println m

        expect:
        m.list.size() == 1
        m2.list.size() == 2
        m2.list[0] == m.list[0]
    }

    def "AddMerge"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        println m

        when:
        m.addMerge(5, 11)
        println m
        then:
        m.list.size() == 1
        m.list[0].begin == 0
        m.list[0].end == 11

        when:
        m.addMerge(20, 30)
        println m
        then:
        m.list.size() == 2
        m.list[1].begin == 20
        m.list[1].end == 30
    }

    def "RemoveSinge"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        when:
        m.removeSinge(101, 200)
        println m
        then:
        m.list.size() == 2

        when:
        m.removeSinge(100, 200)
        println m
        then:
        m.list.size() == 1
        m.list[0].begin == 0
        m.list[0].end == 10
    }

    def "RemoveMerge"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        when:
        m.removeMerge(5, 11)
        println m
        then:
        m.list.size() == 2
        m.list[0].begin == 0
        m.list[0].end == 4

        when:
        m.removeMerge(150, 160)
        println m
        then:
        m.list.size() == 3
        m.list[1].begin == 100
        m.list[1].end == 149
        m.list[2].begin == 161
        m.list[2].end == 200
    }

    def "RemoveSet"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)

        MultiSlotRange m2
        MultiSlotRange m3
        println m

        and:
        TreeSet<Integer> set = []
        TreeSet<Integer> set2 = []
        11.times {
            set << it
            set2 << (it + 110)
        }

        when:
        m2 = m.removeSet(set)
        println m2
        then:
        m2.list.size() == 1
        m2.list[0].begin == 100
        m2.list[0].end == 200

        when:
        m3 = m2.removeSet(set2)
        println m3
        then:
        m3.list.size() == 2
        m3.list[0].begin == 100
        m3.list[0].end == 109
        m3.list[1].begin == 121
        m3.list[1].end == 200
    }

    def "AddSet"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        MultiSlotRange m2
        MultiSlotRange m3

        and:
        TreeSet<Integer> set = []
        TreeSet<Integer> set2 = []
        51.times {
            set << it
            set2 << (it + 300)
        }

        when:
        m2 = m.addSet(set)
        println m2
        then:
        m2.list.size() == 2
        m2.list[0].begin == 0
        m2.list[0].end == 50

        when:
        m3 = m2.addSet(set2)
        println m3
        then:
        m3.list.size() == 3
        m3.list[2].begin == 300
        m3.list[2].end == 350
    }

    def "RemoveSomeFromEnd"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        TreeSet<Integer> r1
        TreeSet<Integer> r2

        when:
        r1 = m.removeSomeFromEnd(50)
        then:
        r1.containsAll 151..200

        when:
        r2 = m.removeSomeFromEnd(110)
        then:
        r2.size() == 110
        r2[-1] == 200
        r2[0] == 2
        r2[9] == 100
    }

    def "Contains"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        expect:
        m.contains(1)
        m.contains(100)
        m.contains(200)

        when:
        m.removeMerge(100, 200)
        println m
        then:
        !m.contains(100)

        when:
        m.removeMerge(5, 11)
        println m
        then:
        m.contains(4)
        !m.contains(5)
    }

    def "ToSet"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        def set = m.toTreeSet()
        def set2 = m.toHashSet()

        expect:
        set.size() == 112
        set[0] == 0
        set[-1] == 200
        set[11] == 100

        set2.size() == 112
        set2[0] == 0
        set2[-1] == 200
        set2[11] == 100
    }

    def "ToList"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        def list = m.toList()

        expect:
        list.size() == 112
        list[0] == 0
        list[-1] == 200
        list[11] == 100
    }

    def "ToString"() {
        given:
        def m = new MultiSlotRange()
        m.addSinge(0, 10)
        m.addSinge(100, 200)
        println m

        expect:
        m.toString() == '0-10,100-200'
    }

    def "FromSelfString"() {
        given:
        def m = MultiSlotRange.fromSelfString('0-10,100-200')
        println m

        expect:
        m.list.size() == 2
        m.list[0].begin == 0
        m.list[0].end == 10
        m.list[1].begin == 100
        m.list[1].end == 200
    }

    def "FromSet"() {
        given:
        TreeSet<Integer> set = []
        TreeSet<Integer> set2 = []
        51.times {
            set << it
            set2 << (it + 300)
        }
        set.addAll set2

        def m = MultiSlotRange.fromSet(set)

        expect:
        m.list.size() == 2
        m.list[0].begin == 0
        m.list[0].end == 50
        m.list[1].begin == 300
        m.list[1].end == 350
    }

    def 'CompareTo'() {
        given:
        TreeSet<Integer> set = []
        TreeSet<Integer> set2 = []
        51.times {
            set << it
            set2 << (it + 300)
        }

        def m = MultiSlotRange.fromSet(set)
        def m2 = MultiSlotRange.fromSet(set2)

        expect:
        m < m2
    }
}
