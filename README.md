# wiki-xml-markdown-converter
This Python script converts large mediawiki xml files into markdown files

## Arguments:

filename: The path to the input xml file (required)
output: The path to the output directory
format: The output format (markdown, html, etc.)
batch-size: The number of articles to process in each batch
resume-from: The position to resume from in the input file
mem-limit: The maximum memory usage in MB

## Example usage:

python convert.py --filename=<FILENAME> --output=<OUTPUT FILENAME> --format=markdown --batch-size=100 --resume-from=0 --mem-limit=100