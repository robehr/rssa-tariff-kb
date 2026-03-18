#!/usr/bin/env python3
"""
RSSA Tariff Knowledge Base Processor
=====================================
Reads 2026 FAQ CSV and four 2019 FAQ CSVs, merges them (2026 takes priority over
2019 where codes overlap), adds billing guideline content from RSSA guidance PDFs,
and outputs a single rssa_knowledge.json file ready to upload to GitHub.

2019 FAQ files:
  - 2019 FAQ - General practice FAQ.csv      (topic / question / answer)
  - 2019 FAQ - Practice management Startup.csv (unstructured startup guidance)
  - 2019 FAQ - Practice Management Q&A.csv   (query / answer)
  - 2019 FAQ - FAQ per code.csv              (tariff_code / description / units / question / answer)

Usage:
    1. Place this script in the same folder as the CSV files
    2. Run: python process_data.py
    3. Upload the generated rssa_knowledge.json to GitHub
"""

import csv
import json
import re
import sys
from pathlib import Path
from datetime import date

# ---------------------------------------------------------------------------
# DEMO MODE
# ---------------------------------------------------------------------------
# Set DEMO_MODE = True  → outputs only FAQ 2019 + emergency callout codes (demo/evaluation build)
# Set DEMO_MODE = False → outputs full knowledge base: FAQ 2026 + FAQ 2019 + all billing guidelines
# Change this ONE line when the full licence is agreed and re-run the script.
DEMO_MODE = True

# ---------------------------------------------------------------------------
# File names (adjust if your files have different names)
# ---------------------------------------------------------------------------
FAQ_2026                 = "FAQs - 20260219184259.csv"
FAQ_2019_GENERAL         = "2019 FAQ - General practice FAQ.csv"
FAQ_2019_STARTUP         = "2019 FAQ - Practice management Startup.csv"
FAQ_2019_PRACTICE_QA     = "2019 FAQ - Practice Management Q&A.csv"
FAQ_2019_PER_CODE        = "2019 FAQ - FAQ per code.csv"
OUTPUT                   = "rssa_knowledge.json"

# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------
STOP_WORDS = {
    'the','a','an','and','or','but','in','on','at','to','for','of','with','by',
    'from','is','are','was','were','be','been','have','has','had','do','does',
    'did','will','would','could','should','may','might','shall','can','this',
    'that','these','those','it','its','not','no','if','as','any','all','only',
    'also','both','either','each','more','most','other','some','such','than',
    'then','when','where','which','who','how','what','why','per','done','used',
    'using','our','we','they','their','you','your','his','her','him','she','he',
    'code','codes','billing','billed','bill','charge','charged','use','used',
}

def clean(text):
    """Remove common UTF-8 encoding artifacts from CSV exports."""
    if not text:
        return ""
    text = str(text)
    fixes = [
        ('\u00e2\u0080\u0099', "'"), ('\u00e2\u0080\u0093', '\u2013'),
        ('\u00e2\u0080\u0094', '\u2014'), ('\u00e2\u0080\u009c', '\u201c'),
        ('\u00e2\u0080\u009d', '\u201d'), ('\u00e2\u0080\u00a2', '\u2022'),
        ('â€™', "'"), ('â€"', '\u2013'), ('â€œ', '"'), ('â€\x9d', '"'),
        ('â€¢', '\u2022'), ('â€¦', '...'), ('Â', ''), ('ï»¿', ''),
    ]
    for old, new in fixes:
        text = text.replace(old, new)
    return ' '.join(text.split()).strip()

def clean_answer(text):
    """
    Remove email-specific artefacts that make no sense in a chatbot context:
    - Sentences referencing file attachments (the original FAQ entries were email replies)
    - 'Response included attachments' rows
    - Parenthetical attachment notes like '(Full Rules attached)'
    """
    if not text:
        return text

    # Remove whole sentences that mention attachments.
    # A 'sentence' here is anything up to (and including) a period, or to end-of-string.
    sentence_patterns = [
        r'[^.]*?\bI have attached\b[^.]*\.?',
        r'[^.]*?\bplease (?:find|see) (?:the )?attached\b[^.]*\.?',
        r'[^.]*?\bsee (?:the )?attached\b[^.]*\.?',
        r'[^.]*?\bas per (?:the )?attached\b[^.]*\.?',
        r'[^.]*?\bcopies? attached\b[^.]*\.?',
        r'[^.]*?\battached (?:for reference|herewith|hereto)\b[^.]*\.?',
        r'[^.]*?\battached (?:account|referral|report|notice|document|file|guideline|recommendation|list)\b[^.]*\.?',
        r'Response included attachments?\s*',
    ]
    for pattern in sentence_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    # Remove parenthetical attachment notes, e.g. '(Full Ethical Rules attached)'
    text = re.sub(r'\([^)]*\battached\b[^)]*\)', '', text, flags=re.IGNORECASE)

    # Collapse any extra whitespace left behind
    text = re.sub(r'[ \t]{2,}', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_codes(text):
    """Extract all 5-digit billing codes from a string."""
    if not text:
        return []
    return list(set(re.findall(r'\b\d{5}\b', text)))

def keywords_from(text):
    """Return meaningful keyword tokens from text."""
    words = re.findall(r'\b[a-zA-Z]{3,}\b', (text or '').lower())
    return list(set(w for w in words if w not in STOP_WORDS))

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def load_faq_2026(filepath):
    """Load the 2026 FAQ CSV: Id, Name, Corresponding Code, Answer"""
    entries = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                faq_id  = clean(row.get('Id', ''))
                question = clean(row.get('Name', ''))
                code_raw = clean(row.get('Corresponding Code', ''))
                answer   = clean_answer(clean(row.get('Answer', '')))

                if not question or not answer:
                    continue

                # Collect all 5-digit codes from all fields
                codes = extract_codes(code_raw) + extract_codes(question) + extract_codes(answer)
                codes = list(set(codes))

                entries.append({
                    'id':           f'faq26_{faq_id}',
                    'type':         'faq',
                    'codes':        codes,
                    'primary_code': codes[0] if codes else '',
                    'question':     question,
                    'answer':       answer,
                    'source':       'FAQ_2026',
                    'priority':     1,
                    'keywords':     [],   # filled later
                })
        print(f"  Loaded {len(entries)} entries from 2026 FAQ")
    except FileNotFoundError:
        print(f"  WARNING: {filepath} not found – skipping 2026 FAQ")
    except Exception as e:
        print(f"  ERROR loading 2026 FAQ: {e}")
    return entries


def load_faq_2019_general(filepath):
    """
    2019 FAQ - General practice FAQ.csv
    Columns: col0=topic, col1=question, col2=(empty), col3=answer
    """
    entries = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            rows = list(csv.reader(f))
        for i, row in enumerate(rows[1:], start=1):
            while len(row) < 3:
                row.append('')
            topic    = clean(row[0])
            question = clean(row[1])
            answer   = clean_answer(clean(row[2]))
            q_text = question or topic
            if not q_text or not answer or len(q_text) < 5 or len(answer) < 5:
                continue
            codes = list(set(
                extract_codes(topic) + extract_codes(question) + extract_codes(answer)
            ))
            entries.append({
                'id':           f'faq19g_{i}',
                'type':         'faq',
                'codes':        codes,
                'primary_code': codes[0] if codes else '',
                'question':     q_text,
                'answer':       answer,
                'source':       'FAQ_2019_General',
                'priority':     2,
                'keywords':     [],
            })
        print(f"  Loaded {len(entries)} entries from 2019 General FAQ")
    except FileNotFoundError:
        print(f"  WARNING: {filepath} not found – skipping 2019 General FAQ")
    except Exception as e:
        print(f"  ERROR loading 2019 General FAQ: {e}")
    return entries


def load_faq_2019_startup(filepath):
    """
    2019 FAQ - Practice management Startup.csv
    Unstructured document – treat entire content as one knowledge entry.
    """
    entries = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            content = f.read()
        text = clean(content)
        if len(text) > 20:
            codes = list(set(extract_codes(text)))
            entries.append({
                'id':           'faq19s_startup',
                'type':         'faq',
                'codes':        codes,
                'primary_code': codes[0] if codes else '',
                'question':     'What are the requirements and guidelines for starting a new radiology practice?',
                'answer':       text,
                'source':       'FAQ_2019_Startup',
                'priority':     2,
                'keywords':     [],
            })
        print(f"  Loaded {len(entries)} entries from 2019 Startup FAQ")
    except FileNotFoundError:
        print(f"  WARNING: {filepath} not found – skipping 2019 Startup FAQ")
    except Exception as e:
        print(f"  ERROR loading 2019 Startup FAQ: {e}")
    return entries


def load_faq_2019_practice_qa(filepath):
    """
    2019 FAQ - Practice Management Q&A.csv
    Columns: col0=Query, col1=Answer
    """
    entries = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                question = clean(row.get('Query', ''))
                answer   = clean_answer(clean(row.get('Answer', '')))
                if not question or not answer or len(question) < 5 or len(answer) < 5:
                    continue
                codes = list(set(extract_codes(question) + extract_codes(answer)))
                entries.append({
                    'id':           f'faq19p_{i}',
                    'type':         'faq',
                    'codes':        codes,
                    'primary_code': codes[0] if codes else '',
                    'question':     question,
                    'answer':       answer,
                    'source':       'FAQ_2019_PracticeQA',
                    'priority':     2,
                    'keywords':     [],
                })
        print(f"  Loaded {len(entries)} entries from 2019 Practice Q&A FAQ")
    except FileNotFoundError:
        print(f"  WARNING: {filepath} not found – skipping 2019 Practice Q&A FAQ")
    except Exception as e:
        print(f"  ERROR loading 2019 Practice Q&A FAQ: {e}")
    return entries


def load_faq_2019_per_code(filepath):
    """
    2019 FAQ - FAQ per code.csv
    Columns: tatiff_code, description, Units, Question, Answer
    """
    entries = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                code_raw    = clean(row.get('tatiff_code', ''))
                description = clean(row.get('RADIOLOGY, EFFECTIVE FROM 1 JANUARY 2019', ''))
                question    = clean(row.get('Question', ''))
                answer      = clean_answer(clean(row.get('Answer', '')))
                if not question or not answer or len(question) < 5 or len(answer) < 5:
                    continue
                codes = list(set(
                    extract_codes(code_raw) + extract_codes(question) + extract_codes(answer)
                ))
                if code_raw and re.match(r'^\d{5}$', code_raw):
                    primary = code_raw
                elif codes:
                    primary = codes[0]
                else:
                    primary = ''
                # Prepend the tariff description to question for context
                q_text = f"[{code_raw}] {question}" if code_raw else question
                a_text = f"{description}\n\n{answer}" if description else answer
                entries.append({
                    'id':           f'faq19c_{i}',
                    'type':         'faq',
                    'codes':        codes,
                    'primary_code': primary,
                    'question':     q_text,
                    'answer':       a_text,
                    'source':       'FAQ_2019_PerCode',
                    'priority':     2,
                    'keywords':     [],
                })
        print(f"  Loaded {len(entries)} entries from 2019 Per-Code FAQ")
    except FileNotFoundError:
        print(f"  WARNING: {filepath} not found – skipping 2019 Per-Code FAQ")
    except Exception as e:
        print(f"  ERROR loading 2019 Per-Code FAQ: {e}")
    return entries


def load_all_faq_2019(script_dir):
    """Load and combine all four 2019 FAQ files."""
    entries = (
        load_faq_2019_general(script_dir / FAQ_2019_GENERAL) +
        load_faq_2019_startup(script_dir / FAQ_2019_STARTUP) +
        load_faq_2019_practice_qa(script_dir / FAQ_2019_PRACTICE_QA) +
        load_faq_2019_per_code(script_dir / FAQ_2019_PER_CODE)
    )
    print(f"  Total 2019 FAQ entries: {len(entries)}")
    return entries


# ---------------------------------------------------------------------------
# Merge: 2026 takes priority where codes overlap
# ---------------------------------------------------------------------------
def merge(entries_2026, entries_2019):
    codes_2026 = set()
    for e in entries_2026:
        codes_2026.update(e['codes'])

    merged = list(entries_2026)
    added = 0
    for e in entries_2019:
        # Skip 2019 entry if ALL its codes are already covered by 2026
        if e['codes'] and all(c in codes_2026 for c in e['codes']):
            continue
        merged.append(e)
        added += 1

    print(f"  Added {added} non-overlapping entries from 2019 FAQ")
    return merged


# ---------------------------------------------------------------------------
# Billing guidelines (hardcoded from the 5 PDF source documents)
# ---------------------------------------------------------------------------
def billing_guidelines():
    """
    Structured entries extracted from the 5 RSSA billing guideline PDFs:
      1. Appropriate Billing - November 2010
      2. Emergency Call Out Codes 01010 and 01020 (Amended) - March 2013
      3. Radiology Afterhours Codes - June 2004
      4. RSSA Recommendations on Billing for Contrast Materials and Disposable Items - August 2024
      5. Use of Code 01070 - Consultation for an Interventional Procedure - May 2025
    """
    return [

        # ── Appropriate Billing – November 2010 ────────────────────────────

        {
            "id": "bg_spine_stress",
            "type": "guideline",
            "codes": ["53100","53110","53120","53130"],
            "primary_code": "53120",
            "question": "When is the full spine study with stress views and pelvis appropriate billing?",
            "answer": (
                "The full spine study with stress views and pelvis should NOT be billed in 100% of cases. "
                "Trauma, suspected osteoporotic fractures, suspected disc lesions, and back-ache where "
                "spondylolysis or spondylolisthesis is not suspected on the initial study hardly justifies "
                "full flexion and extension views. Apply clinical judgement – only bill for studies appropriate "
                "to the presenting symptoms and diagnosis."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_pelvis_with_spine",
            "type": "guideline",
            "codes": ["53100","53110","53120","53130","55100","56120"],
            "primary_code": "53120",
            "question": "Can pelvis be routinely billed together with lumbar spine X-rays?",
            "answer": (
                "There is no indication for the routine addition of pelvis to a lumbar spine examination "
                "for acute or chronic backache. Pelvis may be added when there are clinical indications "
                "and specifically requested by the referring doctor, or at the radiologist's discretion with "
                "specific clinical justification. Verirad figures show the combination is used in 7–95% of "
                "cases; the national average is around 30%. Practices should assess their own profile against "
                "this benchmark. If the orthopaedic surgeon routinely requests pelvis, get a signed study "
                "protocol to provide to funders."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_spine_mri_contrast",
            "type": "guideline",
            "codes": ["51430","52430","53430"],
            "primary_code": "53430",
            "question": "When is contrast indicated for lumbar or cervical spine MRI?",
            "answer": (
                "MRI of the lumbar spine should NOT virtually always be done with contrast. Contrast is "
                "indicated for previous surgery, intra-spinal metastatic lesions, or other mass/inflammatory "
                "lesions. The indications for contrast in routine lumbar and cervical MRI for radicular "
                "symptoms or spinal stenosis are limited. The billing profile of any practice should reflect "
                "this – practices where contrast is used on nearly every spine MRI will attract scrutiny."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_bmd_lateral",
            "type": "guideline",
            "codes": ["50120"],
            "primary_code": "50120",
            "question": "Can the lateral spine view produced during a bone densitometry (BMD) scan be billed separately?",
            "answer": (
                "No. Some models of bone densitometry screening equipment produce a lateral spine view at "
                "the time of the BMD study. This is considered to be part of the BMD study and separate "
                "billing for this image as a spine view is NOT acceptable."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_skeletal_survey",
            "type": "guideline",
            "codes": ["00110","00115"],
            "primary_code": "00110",
            "question": "What are the billing rules for skeletal survey? Can I bill individual body part codes?",
            "answer": (
                "Codes 00110 + 00115 describe a standard skeletal survey. Practices must NOT bill all the "
                "individual codes for the different body parts included in the study (e.g. Skull, cervical "
                "spine, thoracic spine, lumbar spine, pelvis etc.) as separate billing lines. "
                "Such billing is not acceptable and is not supported by RSSA."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_sinuses_codes",
            "type": "guideline",
            "codes": ["13300","13310","13320","13330","13340"],
            "primary_code": "13300",
            "question": "Which code should I use for CT of the paranasal sinuses?",
            "answer": (
                "CT paranasal sinuses billing (updated 2022/2023):\n"
                "• 13300 – CT paranasal sinuses, single plane, limited study: use for routine screening "
                "when referred by ANY practitioner OTHER than an ENT surgeon. Replaces conventional sinus X-rays. "
                "Any GP or specialist (non-ENT) can request this.\n"
                "• 13320 – CT paranasal sinuses, any plane, complete: use when patient is referred by an "
                "ENT surgeon. Radiologist provides additional planes at no extra cost.\n"
                "• 13330 – Reserved for exceptionally complex pre-surgery planning cases only. "
                "Most paranasal sinuses should be billed as 13300 or 13320.\n"
                "• 13340 – CT paranasal sinuses complete with contrast: when referred by a specialist with "
                "indication for contrast and MRI is unavailable or contraindicated.\n"
                "Note: The fact that MDCT allows multiplane reconstruction from one dataset does NOT justify "
                "billing for multiple planes when there is no change in clinical indication."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010 (updated Sept 2022)",
            "priority": 2,
        },
        {
            "id": "bg_brain_diffusion",
            "type": "guideline",
            "codes": ["10411","10421","10431","10441"],
            "primary_code": "10431",
            "question": "When should diffusion MRI of the brain be billed?",
            "answer": (
                "Diffusion MRI brain should NOT be billed routinely in a high percentage of brain studies. "
                "Limit billing for brain plus diffusion to cases where:\n"
                "• Acute ischaemia is a real clinical possibility\n"
                "• MS\n"
                "• Abscesses and other inflammatory conditions\n"
                "• Epidermoid tumours\n"
                "• Some trauma cases\n"
                "The widespread routine use of diffusion has significantly increased MRI spend and brought "
                "MRI fees under intense scrutiny by funders."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_ct_brain_contrast",
            "type": "guideline",
            "codes": ["10310","10320","10325"],
            "primary_code": "10310",
            "question": "When is CT brain pre and post contrast (with contrast) appropriate?",
            "answer": (
                "The indications for CT brain pre and post contrast have shrunk significantly with modern CT "
                "and the widespread use of MRI. Be circumspect when justifying contrast for a CT brain study, "
                "especially in routine cases like trauma, acute-phase ischaemic incident, or headache – "
                "particularly when an MRI is likely to follow."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_knee_views",
            "type": "guideline",
            "codes": ["72100","72105","72110"],
            "primary_code": "72100",
            "question": "When is a full knee study with patella view justified versus a 2-view study?",
            "answer": (
                "A 2-view knee study is appropriate for many cases and should be performed – it is difficult "
                "to justify the full knee study with patella in all cases, especially for younger patients "
                "with minor trauma or where the request is 'knee pain? OA'. "
                "If standing views are required, the AP view can be done standing as part of the knee study "
                "– a separate fee for an additional standing AP view is NOT supported. "
                "Bilateral knees standing on one film do not justify an additional comparative view fee."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_foot_calcaneus",
            "type": "guideline",
            "codes": ["74120","74125","74130","74135"],
            "primary_code": "74120",
            "question": "Can the calcaneus be billed separately when foot views are done?",
            "answer": (
                "The calcaneus view code is for cases where ONLY the calcaneus is imaged. The foot view "
                "already includes the calcaneus. Additional billing for calcaneus with foot views CANNOT be "
                "supported except in exceptional circumstances that must be motivated. "
                "Remember the swings-and-roundabouts principle: a lesser fee does not apply when only 1-2 "
                "views of the foot are done; conversely, in exceptional cases needing more than 3 views, "
                "the standard fee remains appropriate overall."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_paediatric_limbs",
            "type": "guideline",
            "codes": [],
            "primary_code": "",
            "question": "How should paediatric arm and leg imaging be billed when multiple regions fit on one or two films?",
            "answer": (
                "When two films cover a limb from hip to foot or shoulder to hand, practices must NOT bill "
                "separately for every joint and bone included in those films. Although no single code "
                "currently exists for these studies, billing ethics should lead to charging for ONE of the "
                "joints or set of bones rather than 5-6 separate billing lines. "
                "Future coding updates will include single codes for these studies. "
                "Note: Comparative views of the contralateral limb are NOT indicated when a radiologist is "
                "reporting the examination."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_abdomen_pelvis_us",
            "type": "guideline",
            "codes": ["40210"],
            "primary_code": "40210",
            "question": "When should code 40210 (combined ultrasound abdomen and pelvis) be used?",
            "answer": (
                "Code 40210 (combined ultrasound abdomen and pelvis) should NOT be billed as a routine "
                "in all abdominal scans. Evaluation of the pelvis can generally be justified for female "
                "patients. There is little justification for billing this code for a male presenting with "
                "suspected gall stones. If the bladder is empty and very limited pelvic evaluation is "
                "possible – especially when the suspected pathology is in the upper abdomen – the additional "
                "billing for the pelvis is questionable."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_renal_stones_codes",
            "type": "guideline",
            "codes": ["42300","40337","40340"],
            "primary_code": "42300",
            "question": "What is the correct code for renal stones or suspected renal colic – 42300 or 40337?",
            "answer": (
                "Code 42300 (CT of the renal tract for a stone / CT KUB) is the standard code for suspected "
                "renal tract obstruction by a stone. The vast majority of cases can be adequately evaluated "
                "WITHOUT intravenous contrast. "
                "Code 40337 (CT abdomen and pelvis pre and post contrast) should NOT be used routinely for "
                "suspected renal stones – this is not supported as the norm. "
                "Exception: if no stone is found on the unenhanced scan and there is painless haematuria, "
                "a contrasted study may be appropriate (CT IVU / code 40313). "
                "If there is confusion between a distal ureteric stone and a phlebolith, a small volume of "
                "IV contrast (10-20 ml) with repeat pelvic imaging 10-15 minutes later is one solution."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_carotid_doppler",
            "type": "guideline",
            "codes": ["20220","20230"],
            "primary_code": "20220",
            "question": "When should code 20220 versus 20230 be used for carotid Doppler studies?",
            "answer": (
                "Code 20220 is the standard study for routine carotid Doppler – includes carotid and "
                "vertebrals bilaterally. This is the code used by the vast majority of practices for "
                "routine carotid studies. "
                "Code 20230 was originally intended for very limited use: evaluation of transcranial "
                "vascular malformations and fistulas as part of surgical or interventional work-up. "
                "It should NOT be used for routine Doppler studies. "
                "Note: there is no separate code for temporal artery Doppler – the extracranial vascular "
                "tree including temporal arteries is included in code 20220."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_fluoroscopy_positioning",
            "type": "guideline",
            "codes": ["00140"],
            "primary_code": "00140",
            "question": "Can fluoroscopy code 00140 be billed as part of a plain X-ray study where positioning is difficult?",
            "answer": (
                "No. Fluoroscopy code 00140 CANNOT be billed as part of a B&W X-ray study where positioning "
                "is difficult. This has been discussed at RSSA Exco level and is NOT supported at all by RSSA."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },
        {
            "id": "bg_cad_mammo",
            "type": "guideline",
            "codes": [],
            "primary_code": "",
            "question": "Is CAD mammography billing supported by RSSA?",
            "answer": (
                "No. RSSA has NOT supported the CAD Mammo code for several years and applied for its deletion "
                "as early as 2006. Some funders, notably Discovery Health, have stopped paying for this code. "
                "RSSA is not in a position to challenge those decisions. "
                "There is also no additional fee chargeable for tomosynthesis at present."
            ),
            "source": "billing_guidelines",
            "source_doc": "Appropriate Billing – November 2010",
            "priority": 2,
        },

        # ── Emergency Call Out Codes – March 2013 ──────────────────────────

        {
            "id": "bg_callout_01010_01020",
            "type": "guideline",
            "codes": ["01010","01020"],
            "primary_code": "01010",
            "question": "What are the rules for billing emergency call out codes 01010 and 01020?",
            "answer": (
                "Code 01010 – Emergency callout:\n"
                "• Only used when a radiologist is called out TO THE ROOMS to report after normal working hours.\n"
                "• May NOT be used for routine reporting during extended working hours.\n"
                "• Charged ONCE per callout for the FIRST modality reported (plain film, CT, US, MR etc.), "
                "regardless of the number of examinations in that modality.\n\n"
                "Code 01020 – Subsequent cases:\n"
                "• Used when reporting subsequent cases after having been called out for an initial after-hours "
                "procedure. Also used for HOME TELE-RADIOLOGY reporting of an emergency procedure.\n"
                "• May NOT be used for routine reporting during normal or extended working hours.\n"
                "• May be charged once per MODALITY for the same patient during the same callout.\n"
                "• May be charged once per modality for subsequent PATIENTS during the same callout.\n"
                "• May be charged once per modality for telemedicine reporting.\n\n"
                "Definitions:\n"
                "• Normal working hours: 08:30–17:00 Mon–Fri, 08:30–12:00 Saturday\n"
                "• Extended hours: routine extensions of these hours in the rooms\n"
                "• After hours: all hours outside these times and public holidays\n\n"
                "Example – In one callout to rooms:\n"
                "Patient A, X-rays: 01010 × 1\n"
                "Patient A, CT: 01020 × 1\n"
                "Patient A, Ultrasound: 01020 × 1\n"
                "Patient B, CT: 01020 × 1"
            ),
            "source": "billing_guidelines",
            "source_doc": "Emergency Call Out Codes 01010 and 01020 (Amended) – March 2013",
            "priority": 1,
        },

        # ── Radiology Afterhours Codes – June 2004 ─────────────────────────

        {
            "id": "bg_afterhours_zero_rated",
            "type": "guideline",
            "codes": ["01100","01200","01300","01400","01500","01600"],
            "primary_code": "01100",
            "question": "Can after-hours procedure codes 01100, 01200, 01300, 01400, 01500, 01600 be billed to medical aids?",
            "answer": (
                "No. The RSSA Exco agreed to ZERO RATE the after-hours radiographer procedure codes "
                "01100, 01200, 01300, 01400, 01500, 01600 for all medical aid patients. "
                "These codes have no fee attached and medical aids will reject them. "
                "Practices must stop using these codes for medical aid patients.\n\n"
                "For PRIVATE (non-medical aid) patients, you may still bill at whatever rate is appropriate "
                "for your practice.\n\n"
                "IMPORTANT: This zero-rating does NOT affect the radiologist emergency call out codes "
                "01010 and 01020, which remain valid and are funded by at least some medical aids."
            ),
            "source": "billing_guidelines",
            "source_doc": "Radiology Afterhours Codes – June 2004",
            "priority": 2,
        },

        # ── Contrast Materials and Disposable Items – August 2024 ───────────

        {
            "id": "bg_contrast_sep",
            "type": "guideline",
            "codes": ["00090"],
            "primary_code": "00090",
            "question": "How should contrast materials (MRI/CT contrast) be priced and billed?",
            "answer": (
                "Contrast materials are regulated by Act 90 and MUST be billed at the published Single Exit "
                "Price (SEP). Adding any markup or 'administrative fees' to contrast materials is ILLEGAL. "
                "Contrast accounts for approximately 3% of total radiology payout. "
                "Consumables code 00090 is used for contrast; include NAPPI codes as per the NAPPI code file. "
                "For COIDA patients, code 6260 covers the contrast fee. "
                "Contact Medprax (031-904-9200 / karen@medprax.co.za) for NAPPI code pricing."
            ),
            "source": "billing_guidelines",
            "source_doc": "RSSA Recommendations on Billing for Contrast Materials and Disposable Items – August 2024",
            "priority": 1,
        },
        {
            "id": "bg_disposables_markup",
            "type": "guideline",
            "codes": ["00090"],
            "primary_code": "00090",
            "question": "What markup is allowed on disposable items such as angiographic or interventional catheters?",
            "answer": (
                "Disposables (including catheters) are NOT regulated by Act 90. Allowed markups:\n"
                "• Discovery Health: 36% markup, capped at R59.92\n"
                "• Verirad Managed Schemes: 36% markup, capped at R60.00\n"
                "• Medprax default: 35% on consumable items\n\n"
                "Disposables must NOT be used as profit centres. High uncapped markup on expensive catheters "
                "held on consignment constitutes profiteering and is unacceptable. Medical schemes are entitled "
                "to determine their own disposable benefits and are not obligated to pay third-party published "
                "prices (e.g. Medprax default list) without prior agreement. "
                "Disposables currently account for approximately 4–6% of total radiology payout."
            ),
            "source": "billing_guidelines",
            "source_doc": "RSSA Recommendations on Billing for Contrast Materials and Disposable Items – August 2024",
            "priority": 1,
        },
        {
            "id": "bg_minor_disposables",
            "type": "guideline",
            "codes": [],
            "primary_code": "",
            "question": "Can minor disposables like gloves, gowns, masks, ultrasound gel, gauze swabs be billed separately?",
            "answer": (
                "No. RSSA's coding structure has ALWAYS included minor disposables in the examination fee "
                "for imaging and interventional procedures. They must NOT be billed separately.\n\n"
                "Minor disposables that are included in the examination fee:\n"
                "Aprons, Caps, Gowns, Non-sterile nitrile gloves, Masks, Overshoes, Sleeve protectors, "
                "Cotton wool, Dressings, Gauze, Gauze swabs, Plasters, Tape, Draw sheets, Linen savers, "
                "Lubricating gel, Ultrasound gel, Skin cleanser/disinfectants, Ultrasound image paper, "
                "Alcohol swabs.\n\n"
                "Some practices started billing these separately post-COVID – this practice must stop. "
                "Medical schemes are resistant to paying for them, and it damages the profession's reputation. "
                "PPE/consumables should be billed under code 00090 with NAPPI codes (not as separate line items)."
            ),
            "source": "billing_guidelines",
            "source_doc": "RSSA Recommendations on Billing for Contrast Materials and Disposable Items – August 2024",
            "priority": 1,
        },

        # ── Code 01070 – Consultation for Interventional Procedure – May 2025 ─

        {
            "id": "bg_01070_guideline",
            "type": "guideline",
            "codes": ["01070"],
            "primary_code": "01070",
            "question": "When can code 01070 (consultation for an interventional procedure) be billed?",
            "answer": (
                "Code 01070 may be billed by a radiologist for ALL neuro-interventional procedures and "
                "MAJOR non-neuro interventional procedures. (Version 1.11, May 2025)\n\n"
                "It applies to all procedures in the '8' series EXCEPT the following excluded codes:\n"
                "80600, 80605, 80610, 80640, 80645, 80650, 81660, 81680, 82600, 84615, 85620, "
                "86610, 86615, 86620, 86625, 86630, 87653, 87682, 87683.\n\n"
                "ROUTINE / PLANNED cases:\n"
                "• Consultation must be AT LEAST 15 MINUTES\n"
                "• Must occur on a DAY PRECEDING the scheduled procedure\n"
                "• Must be face-to-face or via video meeting\n"
                "• May NOT be billed for brief explanatory discussions immediately before the procedure\n\n"
                "EMERGENCY cases:\n"
                "• May be billed for a detailed consultation immediately prior to the procedure in the "
                "radiology suite or cath lab, where urgency precludes advance consultation.\n\n"
                "For excluded codes where complexity necessitates detailed consultation, code 01070 "
                "should be negotiated with the medical scheme on a case-by-case basis."
            ),
            "source": "billing_guidelines",
            "source_doc": "Use of Code 01070 – Consultation for Interventional Procedure (V1.11, May 2025)",
            "priority": 1,
        },
    ]


# ---------------------------------------------------------------------------
# Build keyword index
# ---------------------------------------------------------------------------
def enrich_keywords(entries):
    for e in entries:
        base = keywords_from(e.get('question','') + ' ' + e.get('answer',''))
        e['keywords'] = list(set(base + e.get('codes', [])))
    return entries


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    script_dir = Path(__file__).parent

    print("RSSA Knowledge Base Processor")
    print("=" * 45)
    print(f"  Mode: {'DEMO (FAQ 2019 + emergency callout only)' if DEMO_MODE else 'FULL (all sources)'}")
    print()

    entries_2019 = load_all_faq_2019(script_dir)

    if DEMO_MODE:
        # Demo build: 2019 FAQ + emergency callout entry only
        demo_guidelines = [e for e in billing_guidelines() if e['id'] == 'bg_callout_01010_01020']
        all_entries = enrich_keywords(entries_2019 + demo_guidelines)
        entries_2026 = []
        guidelines   = demo_guidelines
        source_note  = "DEMO BUILD – FAQ 2019 (4 files) and emergency callout codes only."
    else:
        # Full build: merge both FAQs + all billing guidelines
        entries_2026 = load_faq_2026(script_dir / FAQ_2026)
        merged_faqs  = merge(entries_2026, entries_2019)
        guidelines   = billing_guidelines()
        all_entries  = enrich_keywords(merged_faqs + guidelines)
        source_note  = ("2026 FAQ takes priority over 2019 FAQ where codes overlap. "
                        "2019 FAQ sourced from 4 separate files. "
                        "Billing guidelines extracted from RSSA PDF documents.")

    kb = {
        "version":          "2.0",
        "generated":        str(date.today()),
        "demo_mode":        DEMO_MODE,
        "source_note":      source_note,
        "fallback_message": (
            "There has been no similar query to the RSSA – please contact the RSSA office at "
            "radsoc@iafrica.com or Tel: 011-794-4395 and ask for more information."
        ),
        "total_entries": len(all_entries),
        "entries":       all_entries,
    }

    output_path = script_dir / OUTPUT
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(kb, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*45}")
    print(f"Output:           {output_path}")
    print(f"Total entries:    {len(all_entries)}")
    if DEMO_MODE:
        print(f"  FAQ 2019 (4 files): {len(entries_2019)}")
        print(f"  Emergency callout:  {len(guidelines)}")
        print(f"\n  *** DEMO MODE – set DEMO_MODE = False for full build ***")
    else:
        print(f"  FAQ 2026:           {len(entries_2026)}")
        print(f"  FAQ 2019 (4 files): {len(all_entries) - len(entries_2026) - len(guidelines)}")
        print(f"  Guidelines:         {len(guidelines)}")
    print("\nNext steps:")
    print("  1. Upload rssa_knowledge.json to your GitHub repository")
    print("  2. Get the raw GitHub URL (click Raw button on the file page)")
    print("  3. Paste that URL into rssa_chatbot.html at the DATA_URL variable")
    print("  4. Test the chatbot by opening rssa_chatbot.html in a browser")


if __name__ == '__main__':
    main()
