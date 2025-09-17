import re
import hashlib
from typing import Dict, Any
from bs4 import BeautifulSoup


def heavy_html_processing(content: str, url: str) -> Dict[str, Any]:
    """
    CPU-intensive HTML processing to create GIL bottleneck
    """
    soup = BeautifulSoup(content, 'html.parser')
    
    # 1. Extract all text content
    all_text = soup.get_text()
    
    # 2. Word counting and length calculations
    words = all_text.split()
    word_count = len(words)
    char_count = len(all_text)
    avg_word_length = sum(len(word) for word in words) / max(word_count, 1)
    
    # 3. Extract emails using regex (CPU intensive)
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, content)
    
    # 4. Extract links using regex
    link_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;:]'
    links = re.findall(link_pattern, content)
    
    # 5. Extract phone numbers (more CPU work)
    phone_pattern = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
    phones = re.findall(phone_pattern, content)
    
    # 6. Calculate MD5 hash of content (CPU intensive)
    content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
    
    # 7. Additional CPU-intensive text analysis
    unique_words = len(set(word.lower() for word in words if word.isalpha()))
    
    # 8. Simulate more CPU work - character frequency analysis
    char_freq = {}
    for char in all_text.lower():
        if char.isalpha():
            char_freq[char] = char_freq.get(char, 0) + 1
    
    # 9. Calculate some metrics (more CPU cycles)
    most_common_char = max(char_freq.items(), key=lambda x: x[1]) if char_freq else ('', 0)
    
    # 10. Extract title with more processing
    title = soup.find('title')
    title_text = title.get_text().strip() if title else "No title"
    
    # 11. Count HTML tags (more parsing work)
    all_tags = soup.find_all()
    tag_counts = {}
    for tag in all_tags:
        tag_counts[tag.name] = tag_counts.get(tag.name, 0) + 1
    
    # 12. Extract meta information
    meta_tags = soup.find_all('meta')
    meta_info = {}
    for meta in meta_tags:
        name = meta.get('name') or meta.get('property')
        content = meta.get('content')
        if name and content:
            meta_info[name] = content
    
    return {
        'url': url,
        'title': title_text,
        'content_length': len(content),
        'word_count': word_count,
        'char_count': char_count,
        'unique_words': unique_words,
        'avg_word_length': round(avg_word_length, 2),
        'emails_found': len(emails),
        'links_found': len(links),
        'phones_found': len(phones),
        'content_hash': content_hash,
        'most_common_char': most_common_char[0],
        'char_frequency_count': len(char_freq),
        'html_tags_count': len(tag_counts),
        'meta_tags_count': len(meta_info),
        'processing_complexity_score': word_count + len(emails) + len(links) + len(char_freq)
    }