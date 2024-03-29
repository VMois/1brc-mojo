alias input_file = "../1brc/measurements.txt"
#alias input_file = "small_measurements.txt"
alias chunk_size = 2048 * 2048

fn main() raises:
    with open(input_file, "rb") as f:
        while True:
            var chunk = f.read(chunk_size)
            if len(chunk) < chunk_size:
                break
