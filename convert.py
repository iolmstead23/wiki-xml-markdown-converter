import argparse
import logging
import os
import re
import signal
import sys
import tempfile
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path
import xml.etree.ElementTree as ET
import json
import subprocess

logger = logging.getLogger(__name__)
should_exit = False


def signal_handler(signum):
    global should_exit
    logger.info(f"Received signal {signum}, saving state...")
    should_exit = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', required=True)
    parser.add_argument('--output', default='data')
    parser.add_argument('--format', default='markdown')
    parser.add_argument('--batch-size', type=int, default=100)
    parser.add_argument('--resume-from', type=int, default=0)
    parser.add_argument('--mem-limit', type=int, default=100)
    return parser.parse_args()


def save_state(state_file, processed, success, failed):
    state = {
        'position': processed,
        'success': success,
        'failed': failed,
        'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'terminated' if should_exit else 'in_progress'
    }
    with open(state_file, 'w') as f:
        json.dump(state, f)


def load_state(state_file):
    try:
        with open(state_file, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_markdown(title, content, output_dir):
    try:
        url = title.replace(' ', '_')
        dir_path = os.path.join(output_dir, os.path.dirname(url))
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(output_dir, f"{url}.md")

        print(f"Saving markdown file: {file_path}")
        print(f"Content length: {len(content)}")

        with open(file_path, 'w', encoding='utf-8', errors='replace') as f:
            f.write(
                f"---\ntitle: {title}\npermalink: /{url}/\n---\n\n{content}")

        return True
    except Exception as e:
        logger.error(f"Failed to save {title}: {str(e)}")
        return False


def process_page(element, namespace):
    namespaces = {'ns': namespace}

    title_elem = element.find('ns:title', namespaces)
    text_elem = element.find('ns:revision/ns:text', namespaces)

    valid_title = title_elem is not None and title_elem.text is not None
    valid_text = text_elem is not None and text_elem.text is not None

    logger.debug(f"Title valid: {valid_title}, Text valid: {valid_text}")

    if valid_title and valid_text:
        return (title_elem.text.strip(), text_elem.text.strip())
    return None


def process_single_page(title, text, output_dir, format):
    try:
        print(f"Processing article: {title}")
        print(f"Text length: {len(text)}")

        # Clean the wiki markup
        cleaned = clean_wiki_markup(text)
        print(f"Cleaned text length: {len(cleaned)}")

        # Convert to markdown
        converted = convert_to_markdown(cleaned, format)
        if not converted:
            raise ValueError("Conversion returned empty content")
        print(f"Converted text length: {len(converted)}")

        # Save the markdown file
        success = save_markdown(title, converted, output_dir)
        if not success:
            raise ValueError("Failed to save markdown file")

        return True
    except Exception as e:
        logger.error(f"Failed processing {title}: {str(e)}", exc_info=True)
        return False


def process_wikilink(link):
    if '|' in link:
        target, label = link.split('|', 1)
        return f'[{label}](/{target.replace(" ", "_")})'
    return f'[{link}](/{link.replace(" ", "_")})'


def clean_wiki_markup(text):
    print(f"Original text: {text[:50]}...")

    patterns = [
        (r'\{\| class="wikitable sortable mw-collapsible" ; (text-align:[^"]+)"',
         r'{| class="wikitable sortable mw-collapsible" style="\1"'),
        (r'\|-\s*$', r'|-'),
        (r'\| style="vertical-align: top; \|',
         r'| style="vertical-align: top;" |'),
        (r'data-sheets-value="{"1":2,"2":"([^"]+)"}"',
         r'data-sheets-value="\1"'),
        (r'\{\{short description\|([^}]+)\}\}',
         r'<!-- Short description: \1 -->'),
        (r'\{\{Use [^}]+\}\}', ''),
        (r'<ref name="([^"]+)">\s*<\/ref>', r'<!--ref \1-->'),
        (r'<ref name=\'([^\']+)\'>\s*<\/ref>', r'<!--ref \1-->'),
        (r'\{\{([^}]+)\}\}', r'<!-- \1 -->'),
        (r'\[\[(.+?)\]\]', lambda m: process_wikilink(m.group(1)))
    ]

    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text)

    print(f"Cleaned text: {text[:50]}...")
    return text


def convert_to_markdown(text, format):
    print(f"Input text for conversion: {text[:50]}...")

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
        temp.write(text)
        temp_name = temp.name

    try:
        cmd = ['pandoc', '--from=mediawiki',
               f'--to={format}', '--wrap=none', temp_name]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=30, encoding='utf-8', errors='ignore')

        if result.returncode != 0:
            logger.error(f"Pandoc error: {result.stderr[:200]}")
            return None

        # Debugging (first 100 chars)
        print(f"Converted text: {result.stdout[:50]}...")
        return result.stdout
    except subprocess.TimeoutExpired:
        logger.error("Pandoc timeout")
        return None
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def main():
    args = parse_args()
    os.makedirs(args.output, exist_ok=True)
    os.makedirs(os.path.join(args.output, 'logs'), exist_ok=True)
    log_dir = os.path.join(args.output, 'logs')

    Path(os.path.join(log_dir, '.conversion_state.json')).touch(exist_ok=True)
    Path(os.path.join(log_dir, 'conversion_dev.log')).touch(exist_ok=True)
    Path(os.path.join(log_dir, 'conversion_events.csv')).touch(exist_ok=True)

    # Initialize logging and state
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(os.path.join(log_dir, 'conversion_dev.log')),
        ]
    )

    csv_handler = logging.FileHandler(
        os.path.join(log_dir, 'conversion_events.csv'))
    csv_handler.setFormatter(logging.Formatter('%(asctime)s,%(message)s'))
    logging.getLogger().addHandler(csv_handler)

    # Verify input file
    if not os.path.exists(args.filename):
        logger.error(f"Input file '{args.filename}' does not exist.")
        sys.exit(1)
    print(f"Input file '{args.filename}' verified.")

    state_file = os.path.join(log_dir, '.conversion_state.json')
    state = load_state(state_file)
    processed = state['position'] if state else args.resume_from
    success = state['success'] if state else 0
    failed = state['failed'] if state else 0

    try:
        with open(args.filename, 'rb') as xml_file:
            namespace = None
            context = ET.iterparse(xml_file, events=('start', 'end'))
            root = None

            for event, element in context:
                if should_exit:
                    save_state(state_file, processed, success, failed)
                    sys.exit(0)

                # Detect namespace from root element
                if event == 'start' and namespace is None:
                    namespace = element.tag.split('}')[0].strip('{')
                    logger.info(f"Detected XML namespace: {namespace}")

                    # Store root element for memory management
                    root = element
                    continue

                # Process page elements
                if event == 'end' and element.tag == f'{{{namespace}}}page':
                    page_data = process_page(element, namespace)
                    element.clear()

                    if not page_data:
                        logger.warning(
                            f"Skipping page at position {processed} (no valid data)")
                        failed += 1
                        processed += 1
                        continue

                    # Process batch
                    batch = [page_data]
                    while len(batch) < args.batch_size:
                        try:
                            next_event, next_element = next(context)
                            if next_event == 'end' and next_element.tag == f'{{{namespace}}}page':
                                next_data = process_page(
                                    next_element, namespace)
                                next_element.clear()
                                if next_data:
                                    batch.append(next_data)
                        except StopIteration:
                            break

                    # Process batch in parallel
                    with Pool(cpu_count()) as pool:
                        results = pool.starmap(
                            process_single_page,
                            [(p[0], p[1], args.output, args.format)
                             for p in batch]
                        )

                    # Update counts and state
                    success += sum(results)
                    failed += len(results) - sum(results)
                    processed += len(batch)
                    save_state(state_file, processed, success, failed)
                    logger.info(
                        f"Processed {len(batch)} articles, total: {processed} | Success: {success} | Failed: {failed}")

                    # Clear processed elements to free memory
                    if root is not None:
                        for child in root:
                            root.remove(child)

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(
            f"Unexpected error during XML parsing: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
