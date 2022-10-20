from pbs.cli import make_cli
from pbs.etl.extraction import extract_data


@make_cli
def main(name: str, output_dir: str):
    extract_data(name, output_dir)


if __name__ == "__main__":
    main()
