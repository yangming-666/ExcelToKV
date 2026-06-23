import openpyxl
import tkinter as tk
from tkinter import filedialog, messagebox
import os
import json
import subprocess
import sys
import re
import copy
import kv_to_excel_idempotent_sync

CONFIG_FILE = "config.json"
BACKEND_CONFIG_EXPORT_FILENAME = "configs.json"
BACKEND_CONFIG_MANIFEST_RELATIVE_PATH = os.path.join("utils", "backend_config_specs.txt")
MONSTER_WAVES_RELATIVE_PATH = os.path.join("monsters", "monster_waves.txt")
KV_COMMENT_PATTERN = re.compile(r"//.*?$|/\*.*?\*/", re.MULTILINE | re.DOTALL)
KV_TOKEN_PATTERN = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"|([{}])')
KV_QUOTED_RE = re.compile(r'"([^"]*)"')


def get_runtime_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def run_post_sync_exe():
    sync_exe = os.path.join(get_runtime_dir(), "dzsj-kv-sync.exe")
    if not os.path.isfile(sync_exe):
        return False, f"未找到后续程序：{sync_exe}"

    try:
        subprocess.Popen([sync_exe], cwd=os.path.dirname(sync_exe))
        return True, None
    except Exception as e:
        return False, str(e)

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4)
    except:
        pass
    
def is_commented_row(row):
    if not row:
        return True
    for cell in row:
        if cell not in (None, "", " "):
            return str(cell).strip().startswith("#")
    return True


def _parse_kv_object(tokens, index):
    out = {}
    while index < len(tokens):
        token = tokens[index]
        if token == "}":
            return out, index + 1
        if token == "{":
            raise ValueError(f"KV 语法错误：位置 {index} 出现未预期的 '{{'")

        key = token
        index += 1
        if index >= len(tokens):
            raise ValueError(f"KV 语法错误：键 {key} 缺少值")

        next_token = tokens[index]
        if next_token == "{":
            child, index = _parse_kv_object(tokens, index + 1)
            out[key] = child
            continue
        if next_token == "}":
            raise ValueError(f"KV 语法错误：键 {key} 后出现未预期的 '}}'")

        out[key] = next_token
        index += 1
    return out, index


def parse_kv_text(text):
    sanitized = KV_COMMENT_PATTERN.sub("", text)
    tokens = []
    for string_token, brace_token in KV_TOKEN_PATTERN.findall(sanitized):
        tokens.append(brace_token or string_token)

    if not tokens:
        return {}

    index = 0
    root = {}
    while index < len(tokens):
        key = tokens[index]
        index += 1
        if index >= len(tokens) or tokens[index] != "{":
            raise ValueError(f"KV 语法错误：根节点 {key} 缺少 '{{'")
        child, index = _parse_kv_object(tokens, index + 1)
        root[key] = child
    return root


def split_inline_comment(raw_line):
    in_quote = False
    escaped = False
    for idx, ch in enumerate(raw_line):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == '"':
            in_quote = not in_quote
            continue
        if not in_quote and raw_line[idx:idx + 2] == "//":
            return raw_line[:idx].rstrip(), raw_line[idx:]
    return raw_line.rstrip(), ""


def _ensure_metadata_node(container, *keys):
    node = container
    for key in keys:
        node = node.setdefault(key, {})
    return node


def parse_kv_comments(text):
    metadata = {
        "pre_root": [],
        "root_suffix": "",
        "root_open_comments": [],
        "pk_comments": {},
        "pk_suffix": {},
        "pk_open_comments": {},
        "field_comments": {},
        "field_suffix": {},
        "block_open_comments": {},
        "subfield_comments": {},
        "subfield_suffix": {},
        "footer_comments": [],
    }

    pending_comments = []
    root_seen = False
    stack = []
    pending_key = None
    pending_key_suffix = ""
    current_pk = None
    current_block = None

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("#"):
            pending_comments.append(line)
            continue

        code, suffix = split_inline_comment(line)
        code = code.strip()

        if code == "{":
            if pending_key is None:
                pending_comments.append(line)
                continue

            level = len(stack)
            key = pending_key
            pending_key = None
            decl_suffix = pending_key_suffix
            pending_key_suffix = ""

            if level == 0:
                root_seen = True
                metadata["root_open_comments"] = pending_comments[:]
                metadata["root_suffix"] = decl_suffix
                pending_comments = []
                stack.append("root")
            elif level == 1:
                current_pk = key
                metadata["pk_comments"][current_pk] = pending_comments[:]
                metadata["pk_suffix"][current_pk] = decl_suffix
                metadata["pk_open_comments"][current_pk] = []
                pending_comments = []
                stack.append("pk")
            elif level == 2:
                current_block = key
                _ensure_metadata_node(metadata["field_comments"], current_pk)
                _ensure_metadata_node(metadata["field_suffix"], current_pk)
                metadata["field_comments"][current_pk][current_block] = pending_comments[:]
                metadata["field_suffix"][current_pk][current_block] = decl_suffix
                _ensure_metadata_node(metadata["block_open_comments"], current_pk)
                metadata["block_open_comments"][current_pk][current_block] = []
                pending_comments = []
                stack.append("block")
            else:
                pending_comments = []
                stack.append("block")
            continue

        if code == "}":
            level = len(stack)
            if level == 1:
                metadata["footer_comments"].extend(pending_comments)
                pending_comments = []
            elif level == 2 and current_pk is not None:
                metadata["pk_open_comments"][current_pk] = pending_comments[:]
                pending_comments = []
            elif level == 3 and current_pk is not None and current_block is not None:
                metadata["block_open_comments"][current_pk][current_block] = pending_comments[:]
                pending_comments = []

            if stack:
                closed = stack.pop()
                if closed == "block":
                    current_block = None
                elif closed == "pk":
                    current_pk = None
            continue

        quoted = KV_QUOTED_RE.findall(code)
        if len(quoted) == 1:
            pending_key = quoted[0]
            pending_key_suffix = suffix
            if not root_seen:
                metadata["pre_root"] = pending_comments[:]
                pending_comments = []
            continue

        if len(quoted) >= 2 and current_pk is not None:
            key = quoted[0]
            if current_block is None:
                _ensure_metadata_node(metadata["field_comments"], current_pk)
                _ensure_metadata_node(metadata["field_suffix"], current_pk)
                metadata["field_comments"][current_pk][key] = pending_comments[:]
                metadata["field_suffix"][current_pk][key] = suffix
            else:
                _ensure_metadata_node(metadata["subfield_comments"], current_pk, current_block)
                _ensure_metadata_node(metadata["subfield_suffix"], current_pk, current_block)
                metadata["subfield_comments"][current_pk][current_block][key] = pending_comments[:]
                metadata["subfield_suffix"][current_pk][current_block][key] = suffix
            pending_comments = []
            continue

        pending_comments.append(line)

    if pending_comments:
        metadata["footer_comments"].extend(pending_comments)
    return metadata


def build_excel_kv_model(rows):
    root_name = None
    for row in rows:
        if is_commented_row(row):
            continue
        for cell in row:
            if cell not in (None, "", " "):
                root_name = str(cell).strip()
                break
        if root_name:
            break

    if root_name is None:
        raise ValueError("无法在 Excel 文件中找到有效的 Root 名称（第一个非空、非注释的单元格内容）。")

    header_row_idx = None
    for idx, row in enumerate(rows):
        if is_commented_row(row):
            continue
        header_row_idx = idx
        break

    if header_row_idx is None:
        raise ValueError("Excel 文件必须包含标题行（非注释行）。")

    header = rows[header_row_idx]
    clean_headers = [str(h).strip() if h not in (None, "", " ") else None for h in header]

    primary_key_col = -1
    for i, h in enumerate(clean_headers):
        if h is not None:
            primary_key_col = i
            break

    if primary_key_col == -1:
        raise ValueError("无法在标题行中找到主键列。")

    pks = []
    for row in rows[header_row_idx + 1:]:
        if is_commented_row(row):
            continue
        if row is None or len(row) <= primary_key_col or row[primary_key_col] in (None, "", " "):
            continue

        pk = str(row[primary_key_col]).strip()
        if pk.startswith("#"):
            continue
        if pk.endswith(".0") and pk[:-2].isdigit():
            pk = pk[:-2]

        fields = []
        for col_idx, header_name in enumerate(clean_headers):
            if header_name is None or col_idx == primary_key_col:
                continue
            value = row[col_idx] if col_idx < len(row) else None
            value_str = str(value).strip() if value is not None else ""
            if not value_str:
                continue

            if "|" in value_str or "," in value_str:
                nested_items = []
                for pair in [p.strip() for p in value_str.split(",") if p.strip()]:
                    if "|" in pair:
                        key, val = [p.strip() for p in pair.split("|", 1)]
                        if key and val:
                            nested_items.append((key, val))
                fields.append(("block", header_name, nested_items))
            else:
                fields.append(("value", header_name, value_str))
        pks.append((pk, fields))

    return root_name, pks


def render_kv_with_preserved_comments(root_name, pks, metadata):
    lines = []

    def emit_comment_lines(comment_lines):
        for comment_line in comment_lines or []:
            lines.append(comment_line)

    emit_comment_lines(metadata.get("pre_root"))
    root_suffix = metadata.get("root_suffix", "")
    lines.append(f'"{root_name}"{(" " + root_suffix) if root_suffix else ""}')
    lines.append("{")
    emit_comment_lines(metadata.get("root_open_comments"))

    for pk, fields in pks:
        emit_comment_lines(metadata.get("pk_comments", {}).get(pk))
        pk_suffix = metadata.get("pk_suffix", {}).get(pk, "")
        lines.append(f'\t"{pk}"{(" " + pk_suffix) if pk_suffix else ""}')
        lines.append("\t{")
        emit_comment_lines(metadata.get("pk_open_comments", {}).get(pk))

        for field_type, field_name, field_value in fields:
            field_comments = metadata.get("field_comments", {}).get(pk, {}).get(field_name, [])
            field_suffix = metadata.get("field_suffix", {}).get(pk, {}).get(field_name, "")
            emit_comment_lines(field_comments)

            if field_type == "block":
                lines.append(f'\t\t"{field_name}"{(" " + field_suffix) if field_suffix else ""}')
                lines.append("\t\t{")
                emit_comment_lines(metadata.get("block_open_comments", {}).get(pk, {}).get(field_name))
                for sub_key, sub_value in field_value:
                    sub_comments = metadata.get("subfield_comments", {}).get(pk, {}).get(field_name, {}).get(sub_key, [])
                    sub_suffix = metadata.get("subfield_suffix", {}).get(pk, {}).get(field_name, {}).get(sub_key, "")
                    emit_comment_lines(sub_comments)
                    lines.append(
                        f'\t\t\t"{sub_key}" "{sub_value}"{(" " + sub_suffix) if sub_suffix else ""}'
                    )
                lines.append("\t\t}")
            else:
                lines.append(
                    f'\t\t"{field_name}" "{field_value}"{(" " + field_suffix) if field_suffix else ""}'
                )

        lines.append("\t}")

    emit_comment_lines(metadata.get("footer_comments"))
    lines.append("}")
    return "\n".join(lines) + "\n"


def normalize_config_root(parsed, root_name):
    if root_name and isinstance(parsed.get(root_name), dict):
        return parsed[root_name]
    return parsed


COMPACT_MARKER_KEY = "__compact_v1"
COMPACT_MISSING = {"__compact_missing": 1}
COMPACT_MISSING_SHORT = []
COMPACT_DEFAULT_SHORT = False
COMPACT_NUMBER_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
COMPACT_META_ALIASES = {
    "__compact_v1": "_",
    "keys": "k",
    "rows": "r",
    "number_keys": "n",
    "stage_first": "f",
    "stages": "s",
    "dicts": "d",
    "defaults": "x",
}
COMPACT_STRING_FIELD_KEYWORDS = (
    "name",
    "source",
    "raw",
    "icon",
    "image",
    "model",
    "path",
    "sound",
    "particle",
    "modifier",
    "ability",
    "hero",
    "unit",
    "monster",
    "career",
    "projectile",
)


def is_compact_missing(value):
    return value == COMPACT_MISSING_SHORT or (
        isinstance(value, dict) and value.get("__compact_missing") == 1 and len(value) == 1
    )


def is_compact_default(value):
    return value is COMPACT_DEFAULT_SHORT


def compact_get(node, long_key, default=None):
    if not isinstance(node, dict):
        return default
    if long_key in node:
        return node[long_key]
    short_key = COMPACT_META_ALIASES.get(long_key)
    if short_key and short_key in node:
        return node[short_key]
    return default


def encode_compact_value(value):
    if isinstance(value, str) and COMPACT_NUMBER_PATTERN.match(value):
        if "." in value:
            return float(value)
        return int(value)
    return value


def decode_compact_value(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    return value


def is_safe_number_path(path, values):
    if not path:
        return False
    leaf = str(path[-1] or "")
    lower_leaf = leaf.lower()
    if lower_leaf.endswith("id"):
        return False
    for keyword in COMPACT_STRING_FIELD_KEYWORDS:
        if keyword in lower_leaf:
            return False

    found_value = False
    for value in values:
        if is_compact_missing(value):
            continue
        found_value = True
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            continue
        if not isinstance(value, str) or not COMPACT_NUMBER_PATTERN.match(value):
            return False
    return found_value


def flatten_compact_record(node, prefix=()):
    out = {}
    for key, value in node.items():
        path = prefix + (str(key),)
        if isinstance(value, dict) and value and not is_compact_missing(value):
            out.update(flatten_compact_record(value, path))
        else:
            out[path] = value
    return out


def compact_path_to_json_key(path):
    if len(path) == 1:
        return path[0]
    return list(path)


def compact_json_key_to_path(key):
    if isinstance(key, list):
        return tuple(str(part) for part in key)
    return (str(key),)


def set_compact_path(node, path, value):
    current = node
    for part in path[:-1]:
        current = current.setdefault(part, {})
    current[path[-1]] = value


def compact_flat_rows_from_records(records, row_order=None):
    if row_order is None:
        row_order = list(records.keys())

    flat_rows = {}
    path_first_index = {}
    path_counts = {}
    next_index = 0
    for row_key in row_order:
        row = records[row_key]
        flattened = flatten_compact_record(row)
        if not flattened:
            return None
        flat_rows[row_key] = flattened
        for path in flattened:
            if path not in path_first_index:
                path_first_index[path] = next_index
                next_index += 1
            path_counts[path] = path_counts.get(path, 0) + 1

    paths = sorted(path_first_index.keys(), key=lambda path: (-path_counts[path], path_first_index[path]))
    path_values = {path: [] for path in paths}
    rows = {}
    for row_key in row_order:
        flattened = flat_rows[row_key]
        values = []
        for path in paths:
            if path in flattened:
                raw_value = flattened[path]
                values.append(encode_compact_value(raw_value))
                path_values[path].append(raw_value)
            else:
                values.append(COMPACT_MISSING_SHORT)
                path_values[path].append(COMPACT_MISSING_SHORT)
        while values and is_compact_missing(values[-1]):
            values.pop()
        rows[row_key] = values

    number_keys = []
    for idx, path in enumerate(paths):
        if is_safe_number_path(path, path_values[path]):
            number_keys.append(idx + 1)

    out = {
        "k": [compact_path_to_json_key(path) for path in paths],
        "r": rows,
    }
    if number_keys:
        out["n"] = number_keys
    return out


def compact_monster_wave_stages(stages):
    if not isinstance(stages, dict) or not stages:
        return stages
    if not all(isinstance(key, str) and len(key) == 6 and key.isdigit() for key in stages):
        return stages
    if not all(isinstance(value, dict) for value in stages.values()):
        return stages

    grouped = {}
    for key, row in stages.items():
        stage_index = int(key[:3])
        wave_index = int(key[3:])
        grouped.setdefault(stage_index, {})[wave_index] = row

    stage_indexes = sorted(grouped)
    if stage_indexes != list(range(stage_indexes[0], stage_indexes[-1] + 1)):
        return stages

    row_order = []
    for stage_index in stage_indexes:
        waves = grouped[stage_index]
        wave_indexes = sorted(waves)
        if wave_indexes != list(range(1, len(wave_indexes) + 1)):
            return stages
        for wave_index in wave_indexes:
            row_order.append(f"{stage_index:03d}{wave_index:03d}")

    flat = compact_flat_rows_from_records(stages, row_order)
    if flat is None:
        return stages

    stage_rows = []
    for stage_index in stage_indexes:
        waves = []
        for wave_index in range(1, len(grouped[stage_index]) + 1):
            row_key = f"{stage_index:03d}{wave_index:03d}"
            waves.append(flat["r"][row_key])
        stage_rows.append(waves)

    dicts = {}
    monster_idx = None
    for idx, key in enumerate(flat["k"]):
        if key == "Monster":
            monster_idx = idx
            break
    if monster_idx is not None:
        monsters = []
        monster_ids = {}
        for waves in stage_rows:
            for row in waves:
                if len(row) <= monster_idx or not isinstance(row[monster_idx], str):
                    continue
                monster = row[monster_idx]
                if monster not in monster_ids:
                    monster_ids[monster] = len(monsters) + 1
                    monsters.append(monster)
                row[monster_idx] = monster_ids[monster]
        if monsters:
            dicts["Monster"] = monsters

    defaults = [None] * len(flat["k"])
    for idx, key in enumerate(flat["k"]):
        if key == ["Buff", "TractionResistance_Flat"]:
            defaults[idx] = 3000
            for waves in stage_rows:
                for row in waves:
                    if idx < len(row) and row[idx] == 3000:
                        row[idx] = COMPACT_DEFAULT_SHORT
                    while row and is_compact_missing(row[-1]):
                        row.pop()

    candidate = {
        "_": "stage_wave_rows",
        "k": flat["k"],
        "f": stage_indexes[0],
        "s": stage_rows,
    }
    if flat.get("n"):
        candidate["n"] = flat["n"]
    if dicts:
        candidate["d"] = dicts
    if any(default is not None for default in defaults):
        candidate["x"] = defaults
    original_json = json.dumps(stages, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compact_json = json.dumps(candidate, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(compact_json) < len(original_json):
        return candidate
    return stages


def expand_compact_table(node, keep_numbers=False):
    if isinstance(node, list):
        return [expand_compact_table(value, keep_numbers) for value in node]
    if not isinstance(node, dict):
        return node

    marker = compact_get(node, "__compact_v1")
    if marker == "flat_rows":
        raw_keys = compact_get(node, "keys", [])
        keys = [compact_json_key_to_path(key) for key in raw_keys]
        number_indexes = set((int(idx) - 1) for idx in compact_get(node, "number_keys", []) if isinstance(idx, int))
        rows = compact_get(node, "rows", {})
        defaults = compact_get(node, "defaults", [])
        out = {}
        for row_key, row_values in rows.items():
            row = {}
            if not isinstance(row_values, list):
                raise ValueError(f"压缩配置行必须是数组：{row_key}")
            for idx, value in enumerate(row_values):
                if idx >= len(keys):
                    raise ValueError(f"压缩配置行字段超出表头：{row_key}")
                if is_compact_default(value):
                    if idx < len(defaults) and defaults[idx] is not None:
                        value = defaults[idx]
                    else:
                        raise ValueError(f"压缩配置默认值标记缺少默认值：{row_key}")
                elif is_compact_missing(value):
                    continue
                if keep_numbers and idx in number_indexes:
                    decoded = value
                else:
                    decoded = decode_compact_value(value)
                set_compact_path(row, keys[idx], expand_compact_table(decoded, keep_numbers))
            out[row_key] = row
        return out
    if marker == "stage_wave_rows":
        raw_keys = compact_get(node, "keys", [])
        keys = [compact_json_key_to_path(key) for key in raw_keys]
        number_indexes = set((int(idx) - 1) for idx in compact_get(node, "number_keys", []) if isinstance(idx, int))
        stages = compact_get(node, "stages", [])
        stage_first = int(compact_get(node, "stage_first", 1))
        defaults = compact_get(node, "defaults", [])
        dict_indexes = {}
        dicts = compact_get(node, "dicts", {})
        if isinstance(dicts, dict):
            for dict_key, dict_values in dicts.items():
                if isinstance(dict_values, list):
                    for idx, raw_key in enumerate(raw_keys):
                        if raw_key == dict_key:
                            dict_indexes[idx] = dict_values
                            break
        if not isinstance(stages, list):
            raise ValueError("压缩 monster_waves stages 必须是数组")
        out = {}
        for stage_offset, stage_rows in enumerate(stages):
            if not isinstance(stage_rows, list):
                raise ValueError(f"压缩 monster_waves 关卡必须是数组：{stage_offset}")
            stage_index = stage_first + stage_offset
            for wave_offset, row_values in enumerate(stage_rows):
                if not isinstance(row_values, list):
                    raise ValueError(f"压缩 monster_waves 波次必须是数组：{stage_index}/{wave_offset + 1}")
                row = {}
                for idx, value in enumerate(row_values):
                    if idx >= len(keys):
                        raise ValueError(f"压缩 monster_waves 行字段超出表头：{stage_index}/{wave_offset + 1}")
                    if is_compact_default(value):
                        if idx < len(defaults) and defaults[idx] is not None:
                            value = defaults[idx]
                        else:
                            raise ValueError(f"压缩 monster_waves 默认值标记缺少默认值：{stage_index}/{wave_offset + 1}")
                    elif is_compact_missing(value):
                        continue
                    if idx in dict_indexes:
                        dict_values = dict_indexes[idx]
                        if not isinstance(value, int) or value < 1 or value > len(dict_values):
                            raise ValueError(f"压缩 monster_waves 字典索引无效：{stage_index}/{wave_offset + 1}")
                        value = dict_values[value - 1]
                    elif keep_numbers and idx in number_indexes:
                        value = value
                    else:
                        value = decode_compact_value(value)
                    set_compact_path(row, keys[idx], expand_compact_table(value, keep_numbers))
                out[f"{stage_index:03d}{wave_offset + 1:03d}"] = row
        return out

    return {key: expand_compact_table(value, keep_numbers) for key, value in node.items()}


def compact_table_node(node, min_rows=4, min_saved_bytes=128):
    if isinstance(node, list):
        return [compact_table_node(value, min_rows, min_saved_bytes) for value in node]
    if not isinstance(node, dict):
        return node

    children = {key: compact_table_node(value, min_rows, min_saved_bytes) for key, value in node.items()}
    if len(children) < min_rows:
        return children
    if COMPACT_MARKER_KEY in children or COMPACT_META_ALIASES[COMPACT_MARKER_KEY] in children:
        return children
    if not all(
        isinstance(value, dict)
        and COMPACT_MARKER_KEY not in value
        and COMPACT_META_ALIASES[COMPACT_MARKER_KEY] not in value
        for value in children.values()
    ):
        return children

    flat = compact_flat_rows_from_records(children)
    if flat is None:
        return children

    candidate = {
        "_": "flat_rows",
        "k": flat["k"],
        "r": flat["r"],
    }
    if flat.get("n"):
        candidate["n"] = flat["n"]
    original_json = json.dumps(children, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compact_json = json.dumps(candidate, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(original_json) - len(compact_json) >= min_saved_bytes and len(compact_json) < len(original_json) * 0.95:
        return candidate
    return children


def parse_bool_value(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
    return default


def normalize_manifest_path(path):
    return os.path.normpath(str(path or "").replace("\\", os.sep).replace("/", os.sep))


def find_backend_config_manifest(output_root):
    direct_path = os.path.join(output_root, BACKEND_CONFIG_MANIFEST_RELATIVE_PATH)
    if os.path.isfile(direct_path):
        return direct_path

    for root, dirs, files in os.walk(output_root):
        dirs[:] = [d for d in dirs if not should_ignore_path(output_root, os.path.join(root, d))]
        for filename in files:
            if filename.lower() == "backend_config_specs.txt":
                full_path = os.path.join(root, filename)
                if not should_ignore_path(output_root, full_path):
                    return full_path
    return None


def load_backend_config_specs(output_root):
    manifest_path = find_backend_config_manifest(output_root)
    if not manifest_path:
        raise FileNotFoundError(
            "未找到后端配置清单："
            + os.path.join(output_root, BACKEND_CONFIG_MANIFEST_RELATIVE_PATH)
        )

    with open(manifest_path, "r", encoding="utf-8-sig") as f:
        parsed = parse_kv_text(f.read())
    manifest = parsed.get("BackendConfigSpecs", parsed)

    specs = []
    for config_key, node in manifest.items():
        if not isinstance(node, dict) or not parse_bool_value(node.get("export"), False):
            continue
        rel_path = normalize_manifest_path(node.get("path") or node.get("Path"))
        if not rel_path:
            continue
        root_name = node.get("root") or node.get("Root")
        specs.append({
            "key": config_key,
            "root": root_name,
            "filename": os.path.basename(rel_path),
            "path": rel_path,
            "export": True,
        })
    return specs, manifest_path


def should_ignore_path(root_dir, path):
    try:
        rel_path = os.path.relpath(path, root_dir)
    except ValueError:
        return False
    normalized_rel = os.path.normpath(rel_path).replace('\\', '/').lower()
    
    cfg = load_config()
    ignored_patterns = cfg.get("ignore_paths", ["abilities"])
    for pattern in ignored_patterns:
        norm_pattern = os.path.normpath(pattern).replace('\\', '/').lower()
        if normalized_rel == norm_pattern or normalized_rel.startswith(norm_pattern + "/"):
            return True
    return False


def find_backend_config_files(output_root, specs):
    matched = {}
    by_filename = {}
    for spec in specs:
        by_filename.setdefault(spec["filename"].lower(), []).append(spec)
        if spec.get("path"):
            expected_path = os.path.join(output_root, spec["path"])
            if os.path.isfile(expected_path):
                matched[spec["key"]] = expected_path

    for root, dirs, files in os.walk(output_root):
        dirs[:] = [d for d in dirs if not should_ignore_path(output_root, os.path.join(root, d))]
        for filename in files:
            full_path = os.path.join(root, filename)
            if should_ignore_path(output_root, full_path):
                continue
            lower_name = filename.lower()

            for spec in by_filename.get(lower_name, []):
                if spec.get("path"):
                    expected_path = os.path.normcase(os.path.normpath(os.path.join(output_root, spec["path"])))
                    actual_path = os.path.normcase(os.path.normpath(full_path))
                    if expected_path != actual_path:
                        continue
                matched[spec["key"]] = full_path

    monster_waves_path = os.path.join(output_root, MONSTER_WAVES_RELATIVE_PATH)
    if not os.path.isfile(monster_waves_path):
        monster_waves_path = None

    return matched, monster_waves_path


def export_backend_configs_json(output_root):
    specs, manifest_path = load_backend_config_specs(output_root)
    matched_files, monster_waves_path = find_backend_config_files(output_root, specs)
    missing = [spec["path"] or spec["filename"] for spec in specs if spec["key"] not in matched_files]

    configs = {}
    for spec in specs:
        config_key = spec["key"]
        kv_path = matched_files.get(config_key)
        if not kv_path:
            continue
        with open(kv_path, "r", encoding="utf-8-sig") as f:
            parsed = parse_kv_text(f.read())
        configs[config_key] = normalize_config_root(parsed, spec.get("root"))

    if monster_waves_path:
        with open(monster_waves_path, "r", encoding="utf-8-sig") as f:
            configs["monster_waves"] = parse_kv_text(f.read())
    else:
        missing.append(MONSTER_WAVES_RELATIVE_PATH)

    original_configs = copy.deepcopy(configs)
    if isinstance(configs.get("monster_waves"), dict) and isinstance(configs["monster_waves"].get("Stages"), dict):
        configs["monster_waves"]["Stages"] = compact_monster_wave_stages(configs["monster_waves"]["Stages"])

    compact_payload = {"configs": compact_table_node(configs)}
    expanded_configs = expand_compact_table(compact_payload).get("configs")
    if expanded_configs != original_configs:
        raise ValueError("后端配置压缩校验失败：展开结果与原始配置不一致")

    json_dir = os.path.join(get_runtime_dir(), "JSON")
    os.makedirs(json_dir, exist_ok=True)
    json_path = os.path.join(json_dir, BACKEND_CONFIG_EXPORT_FILENAME)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(compact_payload, f, ensure_ascii=False, separators=(",", ":"))

    return json_path, missing


############################################
#               KV 转换核心
############################################

def ensure_writable_file(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if os.path.exists(path):
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass

def excel_to_kv(excel_path, output_path):
    # Version: 3.1.0
    
    wb = openpyxl.load_workbook(excel_path, data_only=True)
    ws = wb.worksheets[0]
    rows = list(ws.iter_rows(values_only=True))

    root_name, pks = build_excel_kv_model(rows)
    metadata = {}

    if os.path.isfile(output_path):
        try:
            with open(output_path, "r", encoding="utf-8-sig") as f:
                metadata = parse_kv_comments(f.read())
        except Exception:
            metadata = {}

    rendered = render_kv_with_preserved_comments(root_name, pks, metadata)
    ensure_writable_file(output_path)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        f.write(rendered)
        
############################################
#               GUI 部分
############################################

class App:
    def __init__(self, root):
        self.root = root
        root.title("Excel → KV 转换工具")
        self.config = load_config()

        # Excel 路径
        self.excel_label = tk.Label(root, text="Excel 文件：")
        self.excel_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

        self.excel_path_var = tk.StringVar()
        self.excel_entry = tk.Entry(root, textvariable=self.excel_path_var, width=50)
        self.excel_entry.grid(row=0, column=1, padx=5, pady=5)
        
        if "last_excel_dir" in self.config:
            self.excel_path_var.set(self.config["last_excel_dir"])

        self.excel_btn = tk.Button(root, text="选择文件", command=self.select_excel)
        self.excel_btn.grid(row=0, column=2, padx=5, pady=5)
        
        self.excel_folder_btn = tk.Button(root, text="选择文件夹", command=self.select_excel_folder)
        self.excel_folder_btn.grid(row=0, column=3, padx=5, pady=5)

        # 输出目录
        self.output_label = tk.Label(root, text="KV文件目录：")
        self.output_label.grid(row=1, column=0, padx=5, pady=5, sticky="w")

        self.output_path_var = tk.StringVar()
        self.output_entry = tk.Entry(root, textvariable=self.output_path_var, width=50)
        self.output_entry.grid(row=1, column=1, padx=5, pady=5)

        if "last_output_dir" in self.config:
            self.output_path_var.set(self.config["last_output_dir"])
    
        self.output_btn = tk.Button(root, text="选择文件夹", command=self.select_output_folder)
        self.output_btn.grid(row=1, column=2, padx=5, pady=5)

        # 转换按钮
        self.convert_btn = tk.Button(root, text="Excel 转 KV", command=self.convert, width=20, height=2)
        self.convert_btn.grid(row=2, column=1, pady=20)
        
        self.kv_to_excel_btn = tk.Button(root, text="KV 转 Excel", command=self.convert_kv_to_excel, width=20, height=2)
        self.kv_to_excel_btn.grid(row=2, column=2, padx=10, pady=20)

    ############################################
    #        GUI 功能函数
    ############################################

    def select_excel(self):
        initial = self.config.get("last_excel_dir", "")

        file_path = filedialog.askopenfilename(
            title="选择 Excel 文件",
            filetypes=[("Excel Files", "*.xlsx *.xls")],
            initialdir=initial if os.path.isdir(initial) else ""
        )

        if file_path:
            self.excel_path_var.set(file_path)

            # 记录新路径
            excel_dir = os.path.dirname(file_path)
            self.config["last_excel_dir"] = excel_dir
            save_config(self.config)
            
    def select_excel_folder(self):
        initial = self.config.get("last_excel_dir", "")

        folder = filedialog.askdirectory(
            title="选择包含 Excel 的文件夹",
            initialdir=initial if os.path.isdir(initial) else ""
        )

        if folder:
            self.excel_path_var.set(folder)

            self.config["last_excel_dir"] = folder
            save_config(self.config)



    def select_output_folder(self):
        initial = self.config.get("last_output_dir", "")

        folder = filedialog.askdirectory(
            title="选择输出目录",
            initialdir=initial if os.path.isdir(initial) else ""
        )

        if folder:
            self.output_path_var.set(folder)

            # 记录新路径
            self.config["last_output_dir"] = folder
            save_config(self.config)

    def convert(self):
        excel_path = self.excel_path_var.get()
        output_root = self.output_path_var.get()

        if not excel_path:
            messagebox.showerror("错误", "请选择有效的 Excel 文件或文件夹")
            return

        if not output_root or not os.path.isdir(output_root):
            messagebox.showerror("错误", "请选择有效的输出目录")
            return

        # 记住输出目录
        self.config["last_output_dir"] = output_root
        save_config(self.config)

        # ---------------------------------------------------------
        # 判断是文件还是文件夹
        # ---------------------------------------------------------
        excel_files = []

        if os.path.isfile(excel_path):
            # 单文件
            excel_files.append(excel_path)

        elif os.path.isdir(excel_path):
            # 批量模式：扫描文件夹内所有 EXCEL 文件
            for f in os.listdir(excel_path):
                full = os.path.join(excel_path, f)
                if os.path.isfile(full) and f.lower().endswith((".xlsx", ".xls")):
                    excel_files.append(full)

            if not excel_files:
                messagebox.showerror("错误", "该文件夹内没有找到任何 Excel 文件")
                return
        else:
            messagebox.showerror("错误", "路径不是文件也不是文件夹")
            return

        # ---------------------------------------------------------
        # 批量执行转换
        # ---------------------------------------------------------
        success = 0
        failed = []

        for excel_file in excel_files:
            base_name = os.path.splitext(os.path.basename(excel_file))[0]
            target_filename = base_name + ".txt"

            # 递归搜索原始 txt（保持你原来的逻辑）
            matched_path = None
            for root, dirs, files in os.walk(output_root):
                dirs[:] = [d for d in dirs if not should_ignore_path(output_root, os.path.join(root, d))]
                for f in files:
                    full_path = os.path.join(root, f)
                    if should_ignore_path(output_root, full_path):
                        continue
                    if f.lower() == target_filename.lower():
                        matched_path = full_path
                        break
                if matched_path:
                    break

            if not matched_path:
                matched_path = os.path.join(output_root, target_filename)

            # 开始转换
            try:
                excel_to_kv(excel_file, matched_path)
                success += 1
            except Exception as e:
                failed.append(f"{os.path.basename(excel_file)} : {e}")

        if success > 0:
            json_path = None
            export_missing = []
            export_error = None

            try:
                json_path, export_missing = export_backend_configs_json(output_root)
            except Exception as e:
                export_error = str(e)

            launched, err = run_post_sync_exe()
            summary_lines = [f"Excel 转 KV 完成，成功 {success} 个文件。"]

            if failed:
                summary_lines.append(f"失败：{len(failed)} 个文件。")
                summary_lines.append("失败文件列表：")
                summary_lines.extend(failed)
            if json_path:
                summary_lines.append(f"已导出后端配置 JSON：{json_path}")
            if export_missing:
                summary_lines.append("以下配置 TXT 未找到，未写入 JSON：")
                summary_lines.extend(export_missing)
            if export_error:
                summary_lines.append(f"导出后端配置 JSON 失败：{export_error}")
            if not launched:
                summary_lines.append(f"后续同步未启动：{err}")

            if failed or export_missing or export_error or not launched:
                messagebox.showwarning("转换完成", "\n".join(summary_lines))
            else:
                messagebox.showinfo("成功", "\n".join(summary_lines))
        elif failed:
            msg = (
                f"批量转换完成！\n"
                f"成功：0 个\n"
                f"失败：{len(failed)} 个\n\n"
                f"失败文件列表：\n" + "\n".join(failed)
            )
            messagebox.showwarning("转换失败", msg)

    def convert_kv_to_excel(self):
        excel_path = self.excel_path_var.get()
        kv_root = self.output_path_var.get()

        if not excel_path:
            messagebox.showerror("错误", "请选择 Excel 文件或文件夹")
            return

        if not kv_root or not os.path.isdir(kv_root):
            messagebox.showerror("错误", "请选择有效的 KV 输出目录")
            return

        # ---------------------------------------------------------
        # 收集 Excel 文件
        # ---------------------------------------------------------
        excel_files = []

        if os.path.isfile(excel_path):
            if not excel_path.lower().endswith((".xlsx", ".xls")):
                messagebox.showerror("错误", "请选择 Excel 文件")
                return
            excel_files.append(excel_path)

        elif os.path.isdir(excel_path):
            for f in os.listdir(excel_path):
                full = os.path.join(excel_path, f)
                if os.path.isfile(full) and f.lower().endswith((".xlsx", ".xls")):
                    excel_files.append(full)

            if not excel_files:
                messagebox.showerror("错误", "该文件夹内没有找到任何 Excel 文件")
                return
        else:
            messagebox.showerror("错误", "路径不是文件也不是文件夹")
            return

        # ---------------------------------------------------------
        # 对每个 Excel：用“同名 KV”作为数据源
        # ---------------------------------------------------------
        success = 0
        failed = []

        for excel_file in excel_files:
            base_name = os.path.splitext(os.path.basename(excel_file))[0]
            kv_name = base_name + ".txt"

            # 在 KV 输出目录中查找同名 KV
            kv_path = None
            for root, dirs, files in os.walk(kv_root):
                dirs[:] = [d for d in dirs if not should_ignore_path(kv_root, os.path.join(root, d))]
                for f in files:
                    full_path = os.path.join(root, f)
                    if should_ignore_path(kv_root, full_path):
                        continue
                    if f.lower() == kv_name.lower():
                        kv_path = full_path
                        break
                if kv_path:
                    break

            if not kv_path:
                failed.append(f"{base_name} : 未找到对应 KV")
                continue

            try:
                kv_to_excel_idempotent_sync.kv_to_excel_idempotent_sync(kv_path, excel_file)
                success += 1
            except Exception as e:
                failed.append(f"{base_name} : {e}")

        # ---------------------------------------------------------
        # 结果提示
        # ---------------------------------------------------------
        if failed:
            msg = (
                f"KV → Excel 完成！\n"
                f"成功：{success} 个\n"
                f"失败：{len(failed)} 个\n\n"
                f"失败列表：\n" + "\n".join(failed)
            )
            messagebox.showwarning("部分失败", msg)
        else:
            messagebox.showinfo("成功", f"KV → Excel 同步成功！共 {success} 个文件。")

############################################
#               启动程序
############################################

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
