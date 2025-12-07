# renderz_full_stats_scraper.py - PERFECTED VERSION WITH OPTIMIZATIONS

import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional
import re
import csv
import json
import requests
from collections import Counter


BASE_URL = "https://renderz.app/24/player/"

# Get scraper number from environment variable
SCRAPER_NUM = os.environ.get('SCRAPER_NUM', '1')

ASSET_IDS_CSV = f"asset_ids_{SCRAPER_NUM}.csv"
CSV_OUTPUT = f"players_stats_{SCRAPER_NUM}.csv"
SKILLS_JSON_OUTPUT = f"players_skills_{SCRAPER_NUM}.json"
FAILED_IDS_FILE = f"failed_stats_{SCRAPER_NUM}.txt"



# Get scraper number from environment variable
SCRAPER_NUM = os.environ.get('SCRAPER_NUM', '1')

CSV_OUTPUT = f"players_stats_{SCRAPER_NUM}.csv"
SKILLS_JSON_OUTPUT = f"players_skills_{SCRAPER_NUM}.json"
FAILED_IDS_FILE = f"failed_stats_{SCRAPER_NUM}.txt"


# ID RANGE CONFIGURATION
# Supabase key is Supabase service role key for this
# NEW - ADD THIS (put your actual values):
SUPABASE_URL = "https://ugszalubwvartwalsejx.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVnc3phbHVid3ZhcnR3YWxzZWp4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1ODY1ODgzOSwiZXhwIjoyMDc0MjM0ODM5fQ.slNT1R_wiGqzhgBv-eH8TCKggcobrXl4quI1da2D5KY"
SUPABASE_TABLE = "all_cards"

# TRAINING LEVEL CONFIGURATION
MAX_TRAINING_LEVEL = 30
START_TRAINING_LEVEL = 0

# RANK CONFIGURATION
MIN_RANK = 0
MAX_RANK = 5

BATCH_SIZE = 15       # good parallelism, not too aggressive
BATCH_DELAY = 0.05    # tiny pause between batches
LEVEL_DELAY = 0.0     # you only hit level 0
REQUEST_TIMEOUT = 12  # reasonable max wait per request
MAX_RETRIES = 1       # no real retries: one attempt only
RETRY_DELAY = 0.0     # not used when MAX_RETRIES = 1

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

CSV_FIELDS = [
    "player_id", "rank", "training_level", "name", "position", "alternate_position", "team", "league", "nation_region",
    "skill_moves_stars", "strong_foot_side", "strong_foot_stars", "weak_foot_stars",
    "height_ft_in", "height_cm", "weight_kg",
    "work_rate_attack", "work_rate_defense", "date_added",
    "ovr", "stamina_stat",
    "pace", "acceleration", "sprint_speed",
    "shooting", "finishing", "long_shot", "shot_power", "positioning", "volley", "penalties",
    "passing", "short_passing", "long_passing", "vision", "crossing", "curve", "free_kick",
    "dribbling_head", "dribbling", "balance", "agility", "reactions", "ball_control",
    "defending", "marking", "standing_tackle", "sliding_tackle", "awareness", "heading",
    "physical", "strength", "aggression", "jumping",
    "diving", "gk_diving", "gk_positioning", "handling", "gk_handling", "reflexes", "gk_reflexes", "kicking", "gk_kicking", 
    "league_image",
    "skills", "traits", "traits_name", "event", "is_untradable",
    "player_image", "card_background", "nation_flag", "club_flag",
]

SKILL_STAT_MAPPING = {
    'acc': 'acceleration', 'agg': 'aggression', 'agi': 'agility', 'awa': 'awareness',
    'bal': 'balance', 'bac': 'ball_control', 'cro': 'crossing', 'cur': 'curve',
    'dri': 'dribbling', 'div': 'diving', 'fin': 'finishing', 'fre': 'free_kick',
    'gkd': 'gk_diving', 'han': 'gk_handling', 'gkk': 'gk_kicking', 'gkp': 'gk_positioning',
    'ref': 'gk_reflexes', 'han': 'handling', 'hea': 'heading', 'jmp': 'jumping',
    'kic': 'kicking', 'lpa': 'long_passing', 'lsh': 'long_shot', 'mar': 'marking',
    'pac': 'pace', 'pen': 'penalties', 'pos': 'positioning', 'rea': 'reactions',
    'ref': 'reflexes', 'sho': 'shot_power', 'sli': 'sliding_tackle', 'spd': 'sprint_speed',
    'sta': 'stamina', 'stan': 'standing_tackle', 'str': 'strength', 'spa': 'short_passing',
    'vis': 'vision', 'vol': 'volley',
}
#lpa:15,spa:15,vis:15\
#abilityModifiers:{ref:10,str:10,han:10,gkp:10,rea:10,gkd:10}
total_scraped = 0
total_failed = 0
failed_ids = []

def get_player_ids_from_csv() -> List[int]:
    print(f"\n[LOCAL] Fetching asset_ids from {ASSET_IDS_CSV}...")
    asset_ids = []
    try:
        with open(ASSET_IDS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("asset_id"):
                    asset_ids.append(int(row["asset_id"]))
        asset_ids = sorted(set(asset_ids))
        print(f"[LOCAL] ✅ Found {len(asset_ids):,} unique asset_ids")
        return asset_ids
    except Exception as e:
        print(f"[LOCAL] ❌ Error reading {ASSET_IDS_CSV}: {e}")
        return []


def get_existing_player_level_combinations() -> set:
    """Load already scraped player_id + training_level combinations from CSV"""
    existing_combinations = set()
    if os.path.exists(CSV_OUTPUT):
        try:
            with open(CSV_OUTPUT, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'player_id' in row and row['player_id'] and 'training_level' in row:
                        pid = int(row['player_id'])
                        level = int(row['training_level']) if row['training_level'] else 0
                        existing_combinations.add((pid, level))
            print(f"[CSV] ✅ Found {len(existing_combinations)} already scraped player-level combinations")
        except Exception as e:
            print(f"[CSV] ⚠️ Could not read existing CSV: {e}")
    else:
        print(f"[CSV] ℹ️ No existing CSV found - will create new file")
    return existing_combinations

def get_existing_player_rank_combinations() -> set:
    """Load already scraped player_id + rank combinations from JSON"""
    existing_combinations = set()
    if os.path.exists(SKILLS_JSON_OUTPUT):
        try:
            with open(SKILLS_JSON_OUTPUT, 'r', encoding='utf-8') as f:
                all_skills = json.load(f)
            
            # Parse keys like "24022715_R0_L0" to extract player_id and rank
            for key in all_skills.keys():
                try:
                    parts = key.split('_')
                    player_id = int(parts[0])
                    rank = int(parts[1].replace('R', ''))
                    existing_combinations.add((player_id, rank))
                except:
                    continue
            
            print(f"[JSON] ✅ Found {len(existing_combinations)} already scraped player-rank combinations")
        except Exception as e:
            print(f"[JSON] ⚠️ Could not read existing JSON: {e}")
    else:
        print(f"[JSON] ℹ️ No existing JSON found - will create new file")
    return existing_combinations

def get_player_ids_from_database() -> List[int]:
    """Fetch unique asset_ids from Supabase all_cards table"""
    print(f"\n[DATABASE] Fetching asset_ids from {SUPABASE_TABLE}...")
    
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        
        # Select only unique asset_ids
        url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}?select=asset_id"
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            raise Exception(f"API returned {response.status_code}: {response.text}")
        
        data = response.json()
        
        # Extract unique asset_ids
        asset_ids = list(set(row['asset_id'] for row in data if row.get('asset_id')))
        asset_ids.sort()
        
        print(f"[DATABASE] ✅ Found {len(asset_ids):,} unique asset_ids")
        return asset_ids
        
    except Exception as e:
        print(f"[DATABASE] ❌ Error fetching asset_ids: {e}")
        return []


def save_failed_ids(failed_ids: List[int]):
    """Save failed IDs to text file"""
    try:
        with open(FAILED_IDS_FILE, 'w') as f:
            for pid in sorted(failed_ids):
                f.write(f"{pid}\n")
        print(f"[FAILED IDs] ✅ Saved {len(failed_ids)} failed IDs to {FAILED_IDS_FILE}")
    except Exception as e:
        print(f"[FAILED IDs] ❌ Error saving: {e}")

def save_skills_to_json(player_id: int, rank: int, training_level: int, skills_data: str):
    """Save skills data to separate JSON file - with robust validation"""
    # Validate skills_data is not empty or just whitespace
    if not skills_data or not skills_data.strip():
        return
    
    # Validate it's valid JSON before proceeding
    try:
        skills_dict = json.loads(skills_data)
    except json.JSONDecodeError as e:
        # Skills data is not valid JSON - skip silently
        return
    
    # Check if skills_dict actually has skills data
    if not skills_dict or 'skills' not in skills_dict or not skills_dict['skills']:
        return
    
    try:
        # Load existing JSON file
        if os.path.exists(SKILLS_JSON_OUTPUT):
            try:
                with open(SKILLS_JSON_OUTPUT, 'r', encoding='utf-8') as f:
                    all_skills = json.load(f)
            except json.JSONDecodeError:
                # If existing file is corrupted, start fresh
                all_skills = {}
        else:
            all_skills = {}
        
        # Create unique key: player_id + rank + training_level
        key = f"{player_id}_R{rank}_L{training_level}"
        
        # Store skills data
        all_skills[key] = {
            'player_id': player_id,
            'rank': rank,
            'training_level': training_level,
            'available_points': rank,
            'skills': skills_dict
        }
        
        # Save back to file

        # Save back to file (atomic write using temp file)
        try:
            temp_file = SKILLS_JSON_OUTPUT + ".tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(all_skills, f, ensure_ascii=False, indent=2)
            os.replace(temp_file, SKILLS_JSON_OUTPUT)
        except Exception as e:
            print(f"[SAVE ERROR] Player {player_id} R{rank} L{training_level}: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    
    except Exception as e:
        print(f"[SAVE ERROR] Player {player_id} R{rank} L{training_level}: {e}")


def parse_unlock_requirement(requirement_text: str) -> Optional[Dict]:
    """Parse unlock requirement text into structured format"""
    if not requirement_text or "Unlocks" not in requirement_text:
        return None
    
    try:
        # Pattern: "Unlocks after SkillName is LVL3"
        match = re.search(r'Unlocks after (.+?) is LVL(\d+)', requirement_text, re.IGNORECASE)
        if match:
            return {
                'type': 'skill_level',
                'skill_name': match.group(1).strip(),
                'required_level': int(match.group(2)),
                'text': requirement_text
            }
        
        # Pattern: "Unlocks at Rank 3"
        match = re.search(r'Unlocks at Rank (\d+)', requirement_text, re.IGNORECASE)
        if match:
            return {
                'type': 'rank',
                'required_rank': int(match.group(1)),
                'text': requirement_text
            }
        
        # Generic unlock text
        return {
            'type': 'other',
            'text': requirement_text
        }
    
    except:
        return None


def detect_locked_skills(soup: BeautifulSoup) -> Dict[str, Dict]:
    """Detect which skills are locked from HTML and extract skill names"""
    locked_skills = {}
    
    try:
        # Find all skill buttons
        skill_buttons = soup.find_all('button', class_=lambda x: x and 'flex w-full flex-col' in str(x))
        
        for button in skill_buttons:
            try:
                # Get skill image URL
                img = button.find('img')
                if not img or not img.get('src'):
                    continue
                
                skill_image = img.get('src', '')
                
                # Extract skill name from HTML span
                skill_name = None
                name_span = button.find('span', class_=lambda x: x and 'text-gray' in str(x) and 'pb-2' in str(x) and 'text-center' in str(x))
                if name_span:
                    skill_name = name_span.get_text(strip=True)
                
                # Check if locked (has opacity-60 class or lock icon)
                is_locked = False
                unlock_text = None
                
                # Method 1: Check for opacity-60 in button classes
                button_classes = button.get('class', [])
                if 'opacity-60' in button_classes:
                    is_locked = True
                
                # Method 2: Check for lock icon SVG
                lock_icon = button.find('svg')
                if lock_icon and 'M144 144v48H304V144c0-44.2' in str(lock_icon):
                    is_locked = True
                
                # Get unlock requirement text (italic span)
                italic_span = button.find('span', class_=lambda x: x and 'italic' in str(x))
                if italic_span:
                    unlock_text = italic_span.get_text(strip=True)
                    if unlock_text:
                        is_locked = True
                
                # Store locked status and skill name
                if skill_image:
                    locked_skills[skill_image] = {
                        'locked': is_locked,
                        'unlock_requirement_text': unlock_text,
                        'skill_name': skill_name  # NEW: Store HTML skill name
                    }
            
            except:
                continue
    
    except:
        pass
    
    return locked_skills



def parse_skills_from_javascript(html: str, soup: BeautifulSoup) -> str:
    """Extract skills data from JavaScript and merge with HTML lock status"""
    try:
        # Get locked skills from HTML
        locked_skills_dict = detect_locked_skills(soup)
        
        # Find skillsData in JavaScript
        match = re.search(r'skillsData:\s*(\[.*?\])(?=\s*,(?:priceData|auctionable))', html, re.DOTALL)
        if not match:
            return ""
        
        skills_data_str = match.group(1)
        processed_skills = []
        parts = skills_data_str.split('},{skill:')
        
        for idx, part in enumerate(parts):
            if idx == 0:
                part = part.lstrip('[{')
            else:
                part = 'skill:' + part
            if idx == len(parts) - 1:
                part = part.rstrip('}]')
            
            try:
                skill_id_match = re.search(r'id:(\d+)', part)
                skill_id = int(skill_id_match.group(1)) if skill_id_match else 0
                
                image_match = re.search(r'image:"([^"]+)"', part)
                image_url = image_match.group(1) if image_match else ""
                
                # Get skill name from HTML first, fallback to URL parsing
                skill_name = "UNKNOWN"
                if image_url and image_url in locked_skills_dict:
                    html_name = locked_skills_dict[image_url].get('skill_name')
                    if html_name:
                        skill_name = html_name.upper()
                
                # Fallback: parse from URL if HTML name not found
                if skill_name == "UNKNOWN" and image_url:
                    name_match = re.search(r'skill[_/]S\d+[_/](.+?)[_/]\d+', image_url)
                    if name_match:
                        skill_name = name_match.group(1).replace('_', ' ').upper()
                
                # Get lock status from HTML
                is_locked = False
                unlock_requirement = None
                
                if image_url in locked_skills_dict:
                    lock_info = locked_skills_dict[image_url]
                    is_locked = lock_info['locked']
                    if lock_info['unlock_requirement_text']:
                        unlock_requirement = parse_unlock_requirement(lock_info['unlock_requirement_text'])
                
                # More flexible pattern that handles variations - FIXED REGEX
                levels_match = re.search(r'levels:\[(.+?)\](?=\s*\})', part, re.DOTALL)
                if not levels_match:
                    continue

                levels_str = levels_match.group(1)
                processed_levels = []
                
                level_objs = re.findall(r'\{id:\d+,level:\d+,unlockedPositions:\[[^\]]*\],abilityModifiers:\{[^}]+\}\}', levels_str)
                
                for level_obj in level_objs:
                    level_match = re.search(r'level:(\d+)', level_obj)
                    if not level_match:
                        continue
                    level_num = int(level_match.group(1))
                    
                    pos_match = re.search(r'unlockedPositions:\[([^\]]*)\]', level_obj)
                    positions = []
                    if pos_match and pos_match.group(1):
                        positions = re.findall(r'"([^"]+)"', pos_match.group(1))
                    
                    mods_match = re.search(r'abilityModifiers:\{([^}]+)\}', level_obj)
                    boosts = {}
                    if mods_match:
                        for stat_match in re.finditer(r'(\w+):(\d+)', mods_match.group(1)):
                            abbr = stat_match.group(1)
                            val = int(stat_match.group(2))
                            full_name = SKILL_STAT_MAPPING.get(abbr.lower(), abbr)
                            boosts[full_name] = val
                    
                    if boosts:
                        processed_levels.append({
                            'level': level_num,
                            'positions': positions,
                            'boosts': boosts
                        })
                
                # Get JavaScript requirement (prerequisite skill)
                js_requirement = None
                if 'requirement:null' not in part:
                    req_match = re.search(r'requirement:\{skillId:(\d+),level:(\d+)\}', part)
                    if req_match:
                        js_requirement = {
                            'skill_id': int(req_match.group(1)),
                            'level': int(req_match.group(2))
                        }
                
                if processed_levels:
                    processed_skills.append({
                        'id': skill_id,
                        'name': skill_name,
                        'image': image_url,
                        'locked': is_locked,
                        'unlock_requirement': unlock_requirement,
                        'prerequisite': js_requirement,
                        'levels': processed_levels
                    })
            
            except Exception as e:
                continue
        
        # Return JSON string if skills were parsed, otherwise empty string
        if processed_skills:
            return json.dumps({'skills': processed_skills}, ensure_ascii=False)
        return ""
    
    except Exception as e:
        return ""

def parse_player_page(html: str, player_id: int, rank: int, training_level: int, save_skills_to_json_flag: bool = True) -> Optional[Dict]:
    """Parse complete player data"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        player_data = {"player_id": player_id, "rank": rank, "training_level": training_level}

        # 1. NAME
        title = soup.find('title')
        if title:
            name_text = title.get_text(strip=True)
            name = name_text.split('FC Mobile')[0].replace('- RenderZ', '').replace('|', '').strip()
            player_data['name'] = name
        else:
            player_data['name'] = ""

        # 2. OVR
        rating_div = soup.find('div', class_=re.compile('rating'))
        if rating_div:
            ovr_text = rating_div.get_text(strip=True)
            numbers = re.findall(r'\d+', ovr_text)
            for num in numbers:
                if 40 <= int(num) <= 150:
                    player_data['ovr'] = num
                    break
        if 'ovr' not in player_data:
            player_data['ovr'] = ""

        # 3. POSITION
        position_div = soup.find('div', class_='position')
        if position_div:
            player_data['position'] = position_div.get_text(strip=True)
        else:
            player_data['position'] = ""

        # 4. Details
        details = soup.find_all('div', class_=re.compile('details-list-item'))
        for detail in details:
            text = detail.get_text(" ", strip=True)
            
            if 'ALTERNATE POSITION' in text.upper():
                spans = detail.find_all('span')
                if len(spans) >= 2:
                    alt_pos = spans[1].get_text(strip=True)
                    player_data['alternate_position'] = alt_pos
            
            if 'TEAM' in text.upper() and 'team' not in player_data:
                parts = text.split()
                if len(parts) > 1:
                    player_data['team'] = parts[-1]
            
            if 'LEAGUE' in text.upper() and 'league' not in player_data:
                parts = text.split()
                if len(parts) > 1:
                    player_data['league'] = parts[-1]
            
            if ('NATION' in text.upper() or 'REGION' in text.upper()) and 'nation_region' not in player_data:
                parts = text.split()
                if len(parts) > 1:
                    player_data['nation_region'] = parts[-1]
            
            if 'SKILL MOVES' in text.upper():
                match = re.search(r'\((\d)\)', text)
                if match:
                    player_data['skill_moves_stars'] = match.group(1)
            
            if 'STRONG FOOT' in text.upper() and 'WEAK FOOT' in text.upper():
                foot_match = re.search(r'(LEFT|RIGHT)\s*/\s*\((\d)\)', text, re.IGNORECASE)
                if foot_match:
                    player_data['strong_foot_side'] = foot_match.group(1).upper()
                    player_data['weak_foot_stars'] = foot_match.group(2)
                    player_data['strong_foot_stars'] = "5"
                else:
                    player_data['strong_foot_side'] = "RIGHT"
                    player_data['strong_foot_stars'] = "5"
                    player_data['weak_foot_stars'] = "3"
            
            if 'HEIGHT' in text.upper():
                match = re.search(r"(\d)'(\d{1,2}).*?(\d+)\s*cm", text)
                if match:
                    player_data['height_ft_in'] = f"{match.group(1)}'{match.group(2)}\""
                    player_data['height_cm'] = match.group(3)
            
            if 'WEIGHT' in text.upper():
                match = re.search(r'(\d+)\s*kg', text)
                if match:
                    player_data['weight_kg'] = match.group(1)
            
            if 'WORK RATE' in text.upper():
                match = re.search(r'(\w+)\s*/\s*(\w+)', text)
                if match:
                    player_data['work_rate_attack'] = match.group(1)
                    player_data['work_rate_defense'] = match.group(2)
            
            if 'ADDED ON' in text.upper():
                match = re.search(r'Added on\s+(.+)', text, re.I)
                if match:
                    player_data['date_added'] = match.group(1).strip()

        for key in ['alternate_position', 'team', 'league', 'nation_region', 'skill_moves_stars',
                'strong_foot_side', 'weak_foot_stars', 'height_ft_in', 'height_cm',
                'weight_kg', 'work_rate_attack', 'work_rate_defense', 'date_added', 'league_image']:
            if key not in player_data:
                player_data[key] = ""

        if 'strong_foot_stars' not in player_data or player_data['strong_foot_stars'] == "":
            player_data['strong_foot_stars'] = "5"

        # 5. NUMERIC STATS
        stat_names = soup.find_all('span', class_=re.compile('player-stat-name'))
        stat_values = soup.find_all('span', class_=re.compile('player-stat-value'))
        
        stats_dict = {}
        for i, name_elem in enumerate(stat_names):
            if i < len(stat_values):
                stat_name = name_elem.get_text(strip=True).lower().replace(' ', '_')
                stat_value = stat_values[i].get_text(strip=True)
                if stat_value.isdigit():
                    stats_dict[stat_name] = stat_value

        stat_mapping = {
            "pace": "pace", "acceleration": "acceleration", "sprint_speed": "sprint_speed",
            "shooting": "shooting", "finishing": "finishing", "long_shot": "long_shot",
            "shot_power": "shot_power", "positioning": "positioning", "volley": "volley",
            "penalties": "penalties", "passing": "passing", "short_passing": "short_passing",
            "long_passing": "long_passing", "vision": "vision", "crossing": "crossing",
            "curve": "curve", "free_kick": "free_kick", "dribbling": "dribbling",
            "balance": "balance", "agility": "agility", "reactions": "reactions",
            "ball_control": "ball_control", "defending": "defending", "marking": "marking",
            "standing_tackle": "standing_tackle", "sliding_tackle": "sliding_tackle",
            "awareness": "awareness", "heading": "heading", "physical": "physical",
            "strength": "strength", "aggression": "aggression", "jumping": "jumping",
            "stamina_stat": "stamina",
            "diving": "diving", "gk_diving": "gk_diving", "gk_positioning": "gk_positioning",
            "handling": "handling", "gk_handling": "gk_handling", "reflexes": "reflexes",
            "gk_reflexes": "gk_reflexes", "kicking": "kicking", "gk_kicking": "gk_kicking"
        }

        for csv_field, html_stat_key in stat_mapping.items():
            player_data[csv_field] = stats_dict.get(html_stat_key, "")

        # 6. SKILLS
        skills_urls = []
        skills_container = soup.find('div', class_=lambda x: x and 'w-full rounded bg-surface-900 py-2' in str(x))
        if skills_container:
            skill_imgs = skills_container.find_all('img')
            for img in skill_imgs:
                src = img.get('src', '')
                if src and 'skill_' in src:
                    if src not in skills_urls:
                        skills_urls.append(src)
        player_data['skills'] = ",".join(skills_urls) if skills_urls else ""

        # 7. TRAITS
        traits_urls = []
        traits_container = soup.find('div', class_=lambda x: x and 'flex gap-2 w-full flex-wrap justify-center pb-4' in str(x))
        if traits_container:
            all_imgs = traits_container.find_all('img')
            for img in all_imgs:
                src = img.get('src', '')
                if src and ('logo' in src):
                    if src not in traits_urls:
                        traits_urls.append(src)
        player_data['traits'] = ",".join(traits_urls) if traits_urls else ""

        # 7.2 traits_name
        traits_list = []
        traits_container = soup.find('div', class_=lambda c: c and 'flex-wrap' in c and 'pb-4' in c)
        if traits_container:
            spans = traits_container.find_all('span', class_=lambda c: c and 'bg-surface-800' in c)
            for span in spans:
                name = span.get_text(strip=True)
                if name and name not in traits_list:
                    traits_list.append(name)
        player_data['traits_name'] = ", ".join(traits_list) if traits_list else ""

        # 7.3 Dribbling head
        avg_stats = soup.find_all("div", class_=lambda c: c and 'avg-stat' in str(c))
        
        top_stats = {}
        for stat in avg_stats:
            name_el = stat.find("span", class_=lambda c: c and 'player-stat-name' in str(c))
            value_el = stat.find("span", class_=lambda c: c and 'player-stat-value' in str(c))
            if name_el and value_el:
                key = name_el.get_text(strip=True).strip().lower()
                val = value_el.get_text(strip=True).strip()
                top_stats[key] = val

        player_data["dribbling_head"] = top_stats.get("dribbling", "")

        # 8-11. Images
        player_img = soup.find('img', class_='action-shot')
        if player_img:
            player_data['player_image'] = player_img.get('src', '')
        else:
            player_data['player_image'] = ""

        bg_img = soup.find('img', class_='background')
        if bg_img:
            player_data['card_background'] = bg_img.get('src', '')
        else:
            player_data['card_background'] = ""

        nation_img = soup.find('img', class_='nation')
        if nation_img:
            player_data['nation_flag'] = nation_img.get('src', '')
        else:
            player_data['nation_flag'] = ""

        club_img = soup.find('img', alt='Club')
        if club_img:
            player_data['club_flag'] = club_img.get('src', '')
        else:
            player_data['club_flag'] = ""

        # 12. LEAGUE IMAGE
        league_img = soup.find('img', class_='league')
        if league_img:
            player_data['league_image'] = league_img.get('src', '')
        else:
            player_data['league_image'] = ""

        # 13. SKILLS DATA - Save to JSON file with lock status
        if save_skills_to_json_flag:
            skills_data = parse_skills_from_javascript(html, soup)
            if skills_data:
                save_skills_to_json(player_id, rank, training_level, skills_data)


        # 14. EVENT
        event_name = "Unknown"
        event_span = soup.find('span', class_='text-white text-sm text-center')
        if event_span:
            event_text = event_span.get_text(strip=True)
            if event_text:
                event_name = event_text
        player_data['event'] = event_name

        # 15. IS_UNTRADABLE - Check if player is not auctionable
        is_untradable = ""
        market_data_div = soup.find('div', class_='market-data')
        if market_data_div:
            # Check for "not auctionable" text
            span_text = market_data_div.get_text(strip=True).lower()
            if 'not auctionable' in span_text:
                is_untradable = "True"
            # Also check for noauction image as backup
            noauction_img = market_data_div.find('img', alt=lambda x: x and 'not auctionable' in x.lower())
            if noauction_img:
                is_untradable = "True"
        player_data['is_untradable'] = is_untradable


        if not player_data['name']:
            return None

        return player_data

    except Exception as e:
        print(f"[PARSE ERROR] Player {player_id} Rank {rank} Level {training_level}: {e}")
        return None


def is_valid_player(player_data: Optional[Dict]) -> bool:
    """Check if scraped player data is valid (not a failed/non-existent player)"""
    if not player_data:
        return False
    
    # Check for "Filter Players RenderZ" indicator
    if player_data.get('name') == 'Filter Players  RenderZ':
        return False
    
    # Check if name is empty
    if not player_data.get('name') or player_data.get('name').strip() == '':
        return False
    
    # Check if critical fields are missing (indicating invalid page)
    if not player_data.get('position') and not player_data.get('ovr'):
        return False
    
    return True

async def fetch_player_level(session: aiohttp.ClientSession, player_id: int, rank: int,
                             training_level: int, semaphore: asyncio.Semaphore, save_skills: bool = True) -> Optional[Dict]:
    """Fetch and parse player at specific rank and training level"""
    url = f"{BASE_URL}{player_id}?rankUp={rank}&level={training_level}"
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT) as response:
                    if response.status == 404:
                        print(f"[FETCH] Player {player_id} R{rank} L{training_level}: 404 Not Found")
                        return None

                    if response.status != 200:
                        print(f"[FETCH] Player {player_id} R{rank} L{training_level}: HTTP {response.status} (attempt {attempt})")
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None

                    html = await response.text()
                    player_data = parse_player_page(html, player_id, rank, training_level, save_skills)
                    if player_data:
                        return player_data

                    print(f"[FETCH] Player {player_id} R{rank} L{training_level}: parsed but invalid (attempt {attempt})")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                        continue

            except Exception as e:
                print(f"[FETCH-EXC] Player {player_id} R{rank} L{training_level}: {type(e).__name__} {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                return None

    return None


async def scrape_all_levels_and_ranks_for_player(session: aiohttp.ClientSession, player_id: int,
    existing_rank_combinations: set,
    semaphore: asyncio.Semaphore) -> List[Dict]:
    """Scrape player data - OPTIMIZED to 37 URLs per player with full resume support AND FAIL-FAST VALIDATION"""
    results = []
    
    # Check which ranks still need to be scraped for skills
    ranks_to_scrape = [
        rank for rank in range(MIN_RANK, MAX_RANK + 1)
        if (player_id, rank) not in existing_rank_combinations
    ]

    #this is alignment
    # STEP 1: Validate player exists by checking level 0, rank 0 FIRST
    test_data = await fetch_player_level(session, player_id, 0, 0, semaphore, save_skills=False)
    if not is_valid_player(test_data):
        print(f"[SKIP] Player {player_id}: Invalid player detected (Filter Players / No data)")
        return []  # Return empty - player doesn't exist

    
    # STEP 2: Scrape MISSING RANKS at Level 0 for SKILLS DATA (up to 6 URLs)
    if ranks_to_scrape:
        for idx, rank in enumerate(ranks_to_scrape):
            player_data = await fetch_player_level(session, player_id, rank, 0, semaphore)

            # FAIL-FAST: Check if first rank scrape reveals invalid player
            if idx == 0 and not is_valid_player(player_data):
                print(f"[SKIP] Player {player_id}: Invalid player detected at rank {rank}")
                return []  # Stop immediately - player doesn't exist

            if player_data:
                # collect this rank (level 0) for CSV
                results.append(player_data)

            # Skills data is automatically saved by parse_player_page
            await asyncio.sleep(LEVEL_DELAY * 0.3)

        if len(ranks_to_scrape) < 6:
            print(f"[RESUME] Player {player_id}: Scraped {len(ranks_to_scrape)} missing ranks (already had {6 - len(ranks_to_scrape)})")

    # STEP 3: Scrape MISSING LEVELS at Rank 5 for STATS DATA (up to 31 URLs)
    return results

async def main():
    """Main scraping function"""
    global total_scraped, total_failed, failed_ids
    # Reset globals for this run
    total_scraped = 0
    total_failed = 0
    failed_ids = []
    
    print("=" * 70)
    print("🎮 RENDERZ SCRAPER - RANK-ONLY VERSION (6 URLs per player)")
    print("=" * 70)
    
    #existing_combinations = get_existing_player_level_combinations() #you can unlock this if you want training level scraped
    existing_rank_combinations = get_existing_player_rank_combinations()
    # Also check CSV for resume support
    if os.path.exists(CSV_OUTPUT):
        try:
            with open(CSV_OUTPUT, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                csv_count = 0
                for row in reader:
                    if 'player_id' in row and row['player_id'] and 'rank' in row:
                        pid = int(row['player_id'])
                        rank = int(row['rank']) if row['rank'] else 0
                        existing_rank_combinations.add((pid, rank))
                        csv_count += 1
            print(f"[CSV] ✅ Added {csv_count} player-rank combinations from CSV")
        except Exception as e:
            print(f"[CSV] ⚠️ Could not read CSV for resume: {e}")
    all_player_ids = get_player_ids_from_csv()
    if not all_player_ids:
        print("❌ No player IDs found in database!")
        return
    
    # Fast lookup: how many ranks already scraped per player
    counts_by_player = Counter(p for (p, r) in existing_rank_combinations)

    player_ids_needing_work = [
        pid for pid in all_player_ids
        if counts_by_player.get(pid, 0) < 6
    ]
        

    
    print(f"\n📊 Status:")
    print(f" Source: {ASSET_IDS_CSV} (local CSV)")
    print(f" Total unique asset_ids: {len(all_player_ids):,}")
    print(f"   Ranks per player: {MIN_RANK} → {MAX_RANK} (6 ranks)")
    print(f"   Players needing work: {len(player_ids_needing_work):,}")
    
    if not player_ids_needing_work:
        print("\n✅ All players fully scraped!")
        print("=" * 70)
        return
    
    print(f"\n📊 Configuration:")
    print(f"   Batch size: {BATCH_SIZE}")
    print(f"   CSV Output: {CSV_OUTPUT}")
    print(f"   Skills JSON Output: {SKILLS_JSON_OUTPUT}")
    print(f" URLs per new player: 6 (ranks 0–5 at level 0)")
    print("=" * 70)
    
    start_time = datetime.now()
    semaphore = asyncio.Semaphore(BATCH_SIZE)
    processed_players = 0
    
    write_header = not os.path.exists(CSV_OUTPUT)
    if write_header:
        with open(CSV_OUTPUT, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
            writer.writeheader()
    
    connector = aiohttp.TCPConnector(limit=BATCH_SIZE, limit_per_host=BATCH_SIZE)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for i in range(0, len(player_ids_needing_work), BATCH_SIZE):
            batch_ids = player_ids_needing_work[i:i + BATCH_SIZE]
            print(f"[DEBUG] Starting batch {i//BATCH_SIZE + 1}, players {batch_ids[0]}–{batch_ids[-1]}")
            
            tasks = [
                scrape_all_levels_and_ranks_for_player(session, pid, existing_rank_combinations, semaphore)
                for pid in batch_ids
            ]

            #align
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            print(f"[DEBUG] Finished batch {i//BATCH_SIZE + 1}")

            all_level_data = []

            for idx, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    # Task crashed unexpectedly
                    failed_ids.append(batch_ids[idx])
                    total_failed += 1
                    continue

                player_results = result

                if not player_results:
                    # No valid data for this player
                    failed_ids.append(batch_ids[idx])
                    total_failed += 1
                    continue

                all_level_data.extend(player_results)

            total_scraped += len(all_level_data)

            if all_level_data:
                with open(CSV_OUTPUT, mode='a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
                    for player in all_level_data:
                        for field in CSV_FIELDS:
                            if field not in player:
                                player[field] = ""
                        writer.writerow(player)

            #align
            processed_players += len(batch_ids)
            progress_pct = (processed_players / len(player_ids_needing_work)) * 100
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = total_scraped / elapsed if elapsed > 0 else 0

            remaining_players = len(player_ids_needing_work) - processed_players
            eta_seconds = remaining_players / (processed_players / elapsed) if processed_players > 0 else 0
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)

            print(
                f"[PROGRESS] Players: {processed_players:,}/{len(player_ids_needing_work):,} ({progress_pct:.1f}%) | "
                f"✅ {total_scraped:,} levels | ❌ {total_failed:,} players | "
                f"Rate: {rate:.1f} levels/s | ETA: {eta_min}m {eta_sec}s"
            )

            
            if i + BATCH_SIZE < len(player_ids_needing_work):
                await asyncio.sleep(BATCH_DELAY)
    
    if failed_ids:
        save_failed_ids(failed_ids)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print("✅ SCRAPING COMPLETE!")
    print("=" * 70)
    print(f"⏱️  Duration: {int(duration // 60)}m {int(duration % 60)}s")
    print(f"✅ New level records scraped: {total_scraped:,}")
    print(f"❌ Failed players: {total_failed:,}")
    print(f"💾 CSV file: {CSV_OUTPUT}")
    print(f"💾 Skills JSON file: {SKILLS_JSON_OUTPUT}")
    print("=" * 70)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
        if failed_ids:
            save_failed_ids(failed_ids)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        if failed_ids:
            save_failed_ids(failed_ids)