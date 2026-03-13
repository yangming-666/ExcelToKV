import openpyxl
import tkinter as tk
from tkinter import filedialog, messagebox
import os
import json
import subprocess
import sys
import re
import kv_to_excel_idempotent_sync

CONFIG_FILE = "config.json"
BACKEND_CONFIG_EXPORT_FILENAME = "configs.json"
BACKEND_CONFIG_ROOTS = {
    "account_progression.txt": ("account_progression", "AccountProgression"),
    "career_config.txt": ("career_config", "CareerConfig"),
    "challenge_waves.txt": ("challenge_waves", None),
    "hero_base.txt": ("hero_base", None),
    "hero_growth.txt": ("hero_growth", "Growth"),
    "monster_ability.txt": ("monster_ability", None),
    "monster_stats.txt": ("monster_stats", None),
    "shop1.txt": ("shop1", "ShopItems"),
    "shop1_items.txt": ("shop1_items", "Shop1 Items"),
    "shop1_items_meta.txt": ("shop1_items_meta", "Shop1 Items Meta"),
    "meta_growth_shop.txt": ("meta_growth_shop", "MetaGrowthShop"),
    "shop1_items_projectile.txt": ("shop1_items_projectile", None),
    "shop1_items_enhance.txt": ("shop1_items_enhance", None),
    "shop1_items_enhance_passive.txt": ("shop1_items_enhance_passive", None),
    "Stage.txt": ("stage", "Stage"),
}
MONSTER_WAVES_PATTERN = re.compile(r"^monster_waves_(\d+)\.txt$", re.IGNORECASE)
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


def find_backend_config_files(output_root):
    matched = {}
    monster_wave_files = {}

    for root, _, files in os.walk(output_root):
        for filename in files:
            lower_name = filename.lower()
            full_path = os.path.join(root, filename)

            for expected_name in BACKEND_CONFIG_ROOTS:
                if lower_name == expected_name.lower():
                    matched[expected_name] = full_path
                    break

            wave_match = MONSTER_WAVES_PATTERN.match(filename)
            if wave_match:
                monster_wave_files[wave_match.group(1)] = full_path

    return matched, monster_wave_files


def export_backend_configs_json(output_root):
    matched_files, monster_wave_files = find_backend_config_files(output_root)
    missing = [name for name in BACKEND_CONFIG_ROOTS if name not in matched_files]

    configs = {}
    for filename, (config_key, root_name) in BACKEND_CONFIG_ROOTS.items():
        kv_path = matched_files.get(filename)
        if not kv_path:
            continue
        with open(kv_path, "r", encoding="utf-8-sig") as f:
            parsed = parse_kv_text(f.read())
        configs[config_key] = normalize_config_root(parsed, root_name)

    monster_waves = {}
    for level in sorted(monster_wave_files, key=lambda x: int(x)):
        with open(monster_wave_files[level], "r", encoding="utf-8-sig") as f:
            monster_waves[level] = parse_kv_text(f.read())
    if monster_waves:
        configs["monster_waves"] = monster_waves
    else:
        missing.append("monster_waves_*.txt")

    json_dir = os.path.join(get_runtime_dir(), "JSON")
    os.makedirs(json_dir, exist_ok=True)
    json_path = os.path.join(json_dir, BACKEND_CONFIG_EXPORT_FILENAME)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"configs": configs}, f, ensure_ascii=False, indent=2)

    return json_path, missing


############################################
#               KV 转换核心
############################################

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
                for f in files:
                    if f.lower() == target_filename.lower():
                        matched_path = os.path.join(root, f)
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
                for f in files:
                    if f.lower() == kv_name.lower():
                        kv_path = os.path.join(root, f)
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
