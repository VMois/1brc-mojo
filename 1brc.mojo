from math.math import abs, min, max, trunc, round
from algorithm.sort import sort
from string_dict import Dict as CompactDict
from algorithm import parallelize
from algorithm.functional import num_physical_cores
from os import SEEK_CUR
import os.fstat
import sys

alias input_file = "measurements.txt"
# alias input_file = "small_measurements.txt"

alias cores = 64
alias DEFAULT_SIZE = 64 * 1024 
alias chunk_size = DEFAULT_SIZE

@value
struct Measurement(Stringable):
    var name: String
    var min: Int16
    var max: Int16
    var sum: Int
    var count: Int

    fn __str__(self) -> String:
        return (
            String("name")
            + self.name
            + "\nMax:"
            + self.max
            + "\nmin"
            + self.min
            + "\nsum"
            + self.sum
            + "\ncount"
            + self.count
        )


@always_inline
fn raw_to_float(raw_value: StringRef) -> Int16:
    var p = raw_value.data

    var x: SIMD[DType.int8, 4]
    if raw_value[0] == "-":
        x = raw_value.data.load[width=4](1) - 48
    else:
        x = raw_value.data.load[width=4](0) - 48

    var mask = x >= 0 and x <= 9

    var val: Int16 = 0
    for i in range(0, 4, 1):
        if mask[i]:
            val = val * 10 + int(x[i])
    if raw_value[0] == "-":
        val = val * -1
    return val


fn format_float(value: Float32) -> String:
    return String(int(trunc(value))) + "." + int(abs(value * 10) % 10)


@always_inline
fn format_int(value: Int) -> String:
    var sign = ""
    if value < 0:
        sign = "-"
    return sign + String(abs(value) // 10) + "." + abs(value) % 10


@always_inline
fn swap(inout vector: List[String], a: Int, b: Int):
    var tmp = vector[a]
    vector[a] = vector[b]
    vector[b] = tmp


@always_inline
fn tagger[
    num_workers: Int = 8
](chunk: StringRef, substr: StringRef = "\n") -> List[Int]:

    var indicies = List[Int]()
    indicies.append(0)
    var last_index = chunk.rfind(substr)
    var leap = int(last_index / num_workers)
    var offset = 0
    for i in range(num_workers):
        indicies.append(chunk.find(substr, offset + leap))
        offset += leap
    return indicies


@always_inline
fn process_line(line: StringRef, inout aggregator: CompactDict[Measurement]):
    var name_loc = line.find(";")
    var name = StringRef(line.data, name_loc + 1)
    var raw_value = StringRef(line.data + name_loc + 1, len(line) - len(name))
    var value = raw_to_float(raw_value)

    # Maybe can be streamlined?
    var measurement = aggregator.get(
        name, default=Measurement(name, value, value, 0, 0)
    )
    measurement.min = min(measurement.min, value)
    measurement.max = max(measurement.max, value)
    measurement.sum += int(value)
    measurement.count += 1

    aggregator.put(name, measurement)


@always_inline
fn worker(chunk: StringRef):
    var aggregator = CompactDict[Measurement](200)
    var p = chunk.data
    var head = 0
    var max = int(chunk.data.address + chunk.length)
    while True:
        var line_loc = chunk.find("\n", head)

        if line_loc == -1:
            break

        if line_loc > max:
            break

        var line = StringRef(p + head, line_loc - head)
        process_line(line, aggregator)
        head = line_loc + 1

@always_inline
fn parallelizer[workers: Int = 8](chunk: StringRef) -> Int:
    var indcies = tagger[workers](chunk)

    # var aggr_list = List[CompactDict[Measurement]]()
    # for i in range(workers + 5):
    #     aggr_list.append(CompactDict[Measurement](capacity = 2000))
    # print(len(aggr_list))

    @parameter
    fn inner(index: Int):
        var str_ref = StringRef(chunk.data + indcies[index], indcies[index+1] - indcies[index])
        worker(str_ref)
    parallelize[inner](workers)
    
    # TODO: Find a way to combine the aggregator

    return indcies[len(indcies) -1]


fn main() raises:
    var consumed: UInt64 = 0
    var f = open(input_file, "r")
    var buf = DTypePointer[DType.int8]().alloc(chunk_size)

    var stat = fstat.stat(input_file)
    var size =  stat.st_size 

    while True:
        var chunk = f.read(buf, chunk_size)
        consumed += chunk_size
        var ref = StringRef(buf, chunk_size)
        if consumed >=size:
            break
        var regress = parallelizer[workers = cores](ref)
        consumed = f.seek(chunk_size -regress, whence = 1)
    