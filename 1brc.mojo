from math import min, max, trunc, abs, round
from algorithm.sort import sort
# from string_dict import Dict as CompactDict

alias input_file = "measurements.txt"
#alias input_file = "small_measurements.txt"
alias chunk_size = 2048 * 2048


@value
struct Measurement:
    var name: String
    var min: Int
    var max: Int
    var sum: Int
    var count: Int


@always_inline
fn raw_to_float(raw_value: StringRef) raises -> Int8:
    var p = raw_value.data

    var x: SIMD[DType.int8, 4]
    if raw_value[0] == "-":
        x = raw_value.data.load[width = 4](1)
    else:
        x = raw_value.data.load[width = 4](0)

    var mask = x >= 48 and x <= 57

    var val: Int8 = 0
    for i in range(4):
        if mask[i]:
            val += x[i] * 10^i
    if raw_value[0] == "-":
        val = val - 255
    return val


# fn format_float(value: Float32) -> String:
#     return String(trunc(value).to_int()) + "." + (abs(value * 10) % 10).to_int()


# @always_inline
# fn format_int(value: Int) -> String:
#     var sign = ""
#     if value < 0:
#         sign = "-"
#     return sign + String(abs(value) // 10) + "." + abs(value) % 10


# @always_inline
# fn _partition(inout vector: List[String], low: Int, high: Int) -> Int:
#     var pivot = vector[high]
#     var i = low - 1
#     for j in range(low, high):
#         if vector[j] <= pivot:
#             i += 1
#             swap(vector, i, j)
#     swap(vector, i + 1, high)
#     return i + 1


@always_inline
fn swap(inout vector: List[String], a: Int, b: Int):
    var tmp = vector[a]
    vector[a] = vector[b]
    vector[b] = tmp


# fn _quick_sort(inout vector: List[String], low: Int, high: Int):
#     if low < high:
#         var pi = _partition(vector, low, high)
#         _quick_sort(vector, low, pi - 1)
#         _quick_sort(vector, pi + 1, high)


# fn quick_sort(inout vector: List[String]):
#      _quick_sort(vector, 0, len(vector) - 1)


fn main() raises:
    var prev_line: String = ""
    #var data = CompactDict[Measurement](capacity=200)
    with open(input_file, "r") as f:
        # process chunk
        while True:
            var chunk = f.read(chunk_size)

            var p = chunk._buffer.data
            var current_offset = 0

            # process line
            while True:
                var loc = chunk.find("\n", current_offset)
                if loc == -1:
                    #Return to the last line, avoids additional allocation of new string
                    _ = f.seek(offset = current_offset - chunk_size, whence = 1)
                    break

                var ref = StringRef(p + current_offset, loc - current_offset)
                var name_loc = ref.find(";")
                var name = StringRef(p + current_offset, name_loc)
                var raw_value = StringRef(p + current_offset + name_loc + 1, len(ref) - len(name)) 
                var value = raw_to_float(raw_value)


                # var measurement = data.get(name, default=Measurement(name, value, value, 0, 0))
                # measurement.min = min(measurement.min, value)
                # measurement.max = max(measurement.max, value)
                # measurement.sum += value
                # measurement.count += 1
                # data.put(name, measurement)

                # Advance our search offset past the delimiter
                current_offset = loc + 1

            if len(chunk) < chunk_size:
                break

    # # sort data by name
    # var names = List[String]()
    # for m in data.values:
    #     names.append(m[].name)
    # # quick_sort(names)

    # var res: String = "{"
    # for name in names:
    #     var measurement = data.get(name[], default=Measurement(name[], 0, 0, 0, 0))
    #     res += measurement.name + "=" + format_int(measurement.min) + "/" + format_float((measurement.sum / measurement.count) / 10) + "/" + format_int(measurement.max) + ", "
    # res += "}"
    # print(res)
