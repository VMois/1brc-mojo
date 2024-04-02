from math import min, max, trunc, abs
from algorithm.sort import sort

#alias input_file = "../1brc/measurements.txt"
alias input_file = "small_measurements.txt"
alias chunk_size = 2048 * 2048


@value
struct Measurement:
    var name: String
    var min: Float32
    var max: Float32
    var sum: Float32
    var count: Int


@always_inline
fn raw_to_float(raw_value: String) raises -> Float32:
    var p = raw_value._buffer.data

    var offset: Int = 0
    var sign = 1
    if raw_value[0] == "-":
        sign = -1
        offset = 1

    # Exclude the decimal point and dot
    var part_1 = StringRef((p + offset).value, len(raw_value) - (3 + offset))
    var integer_part = int(part_1[0])
    if len(part_1) == 2:
        integer_part = integer_part * 10 + int(part_1[1])

    # We always have a single decimal place, no need to guess
    var part_2 = StringRef((p + len(raw_value) - 2).value, 1)
    var decimal_part = int(part_2)

    return sign * (integer_part + decimal_part / 10)


@always_inline
fn format_float(value: Float32) -> String:
    return String(trunc(value).to_int()) + "." + (abs(value * 10) % 10).to_int()


@always_inline
fn _partition(inout vector: List[String], low: Int, high: Int) -> Int:
    var pivot = vector[high]
    var i = low - 1
    for j in range(low, high):
        if vector[j] <= pivot:
            i += 1
            swap(vector, i, j)
    swap(vector, i + 1, high)
    return i + 1


@always_inline
fn swap(inout vector: List[String], a: Int, b: Int):
    var tmp = vector[a]
    vector[a] = vector[b]
    vector[b] = tmp


fn _quick_sort(inout vector: List[String], low: Int, high: Int):
    if low < high:
        var pi = _partition(vector, low, high)
        _quick_sort(vector, low, pi - 1)
        _quick_sort(vector, pi + 1, high)


fn quick_sort(inout vector: List[String]):
     _quick_sort(vector, 0, len(vector) - 1)


fn main() raises:
    var prev_line: String = ""
    var data = Dict[String, Measurement]()
    with open(input_file, "rb") as f:
        while True:
            var chunk = f.read(chunk_size)
            chunk = prev_line + chunk

            var p = chunk._buffer.data
            prev_line = ""
            var current_offset = 0

            while True:
                var loc = chunk.find("\n", current_offset)
                if loc == -1:
                    prev_line = chunk[current_offset:]
                    break

                var ref = StringRef((p + current_offset).value, loc - current_offset)
                var name_loc = ref.find(";")
                var name = StringRef((p + current_offset).value, name_loc)
                var raw_value = StringRef((p + current_offset + name_loc + 1).value, len(ref) - len(name)) 
                
                var value = raw_to_float(raw_value)
                #print(name, value)

                # if name in data:
                #     var measurement = data[name]
                #     measurement.min = min(measurement.min, value)
                #     measurement.max = max(measurement.max, value)
                #     measurement.sum += value
                #     measurement.count += 1
                #     data[name] = measurement
                # else:
                #     data[name] = Measurement(name, value, value, value, 1)

                # Advance our search offset past the delimiter
                current_offset = loc + len("\n")

            if len(chunk) < chunk_size:
                break

    # sort data by name
    # var names = List[String]()
    # for name in data.keys():
    #     names.append(name[])
    # quick_sort(names)

    # var res: String = "{"
    # for name in names:
    #     var measurement = data[name[]]
    #     res += name[] + "=" + format_float(measurement.min) + "/" + format_float(measurement.sum / Float32(measurement.count)) + "/" + format_float(measurement.max) + ", "
    # res += "}"
    # print(res)
