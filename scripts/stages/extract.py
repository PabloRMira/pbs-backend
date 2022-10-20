from pbs.cli import make_cli
from pbs.etl.extraction import extract_data


@make_cli
def main(output_dir: str):
    extract_data(output_dir)


if __name__ == "__main__":
    main()
