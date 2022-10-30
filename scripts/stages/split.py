from pbs.cli import make_cli
from pbs.etl.split import split_data


@make_cli
def main(input_path: str, output_path: str):
    split_data(input_path, output_path)


if __name__ == "__main__":
    main()
