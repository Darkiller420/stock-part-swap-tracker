import os
import uuid
from datetime import datetime
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from collections import defaultdict
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_bcrypt import Bcrypt

# --- App Initialization ---
app = Flask(__name__, template_folder='templates')
app.secret_key = 'super_secret_key_for_flash_messages'
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

# --- Database Configuration (Unchanged) ---
DB_DRIVER = 'ODBC+Driver+17+for+SQL+Server'
DB_HOST = os.environ.get('DB_HOST', 'localhost,50567')
DB_NAME = os.environ.get('DB_NAME', 'warehouseSC')
DB_USER = os.environ.get('DB_USER', 'dev')
DB_PASS = os.environ.get('DB_PASS')

if not DB_PASS:
    print("Warning: DB_PASS environment variable is not set. Using fallback for local dev.")
    DB_PASS = 'UDTonline2026!!'

app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}'
# --- Added configuration to suppress modification tracking warning ---
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# --- End Added configuration ---
db = SQLAlchemy(app)

# --- Flask-Login & Bcrypt Initialization (Unchanged) ---
bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'


# --- Global Configuration (Unchanged) ---
COLUMN_MAP = {
    # ... (remains unchanged) ...
    'request_id': 'Request ID',
    'udt_ticket_wo': 'UDT TICKET/WO',
    'part_abbreviation': 'PART',
    'serial_num': 'SERIAL',
    'oem_claim_num': 'OEM Claim #',
    'date_requested': 'Date Requested',
    'status': 'Status',
    'stock_part_used_sku': 'Dispatched SKU',
    'inven_adjust': 'Inventory Adjustment',
    'stock_bin': 'Dispatch Bin',
    'dispatch_doa': 'Dispatch DOA',
    'date_dispatched': 'Date Dispatched',
    'received_part_sku': 'Received SKU',
    'received_ppid': 'Received PPID',
    'received_qty': 'Received Qty',
    'received_bin': 'Received Bin',
    'received_doa': 'Received DOA',
    'date_replenished': 'Date Replenished',
    'inventory_id': 'Inventory ID',
    'inventory_date': 'Date',
    'part_sku': 'Part SKU',
    'quantity': 'Quantity',
    'log_type': 'Type',
    'bin': 'Bin Location',
    'notes': 'Notes',
    'related_request_id': 'Related Request ID',
    'part_acronym': 'Part Acronym'
}
# --- NEW: Define items per page for pagination ---
ITEMS_PER_PAGE = 20
# --- END NEW ---

# --- Database Models (Unchanged) ---

class SwapRequest(db.Model, UserMixin): # Added UserMixin here by mistake in previous edits, removing it.
# Corrected UserMixin placement:
# class SwapRequest(db.Model): # Correct
    # ... (SwapRequest model definition remains unchanged) ...
    __tablename__ = 'swap_request'
    request_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    udt_ticket_wo = db.Column(db.String(100), nullable=False)
    part_abbreviation = db.Column(db.String(50), nullable=False)
    serial_num = db.Column(db.String(100), nullable=False)
    oem_claim_num = db.Column(db.String(100), nullable=True, default='')
    date_requested = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    status = db.Column(db.String(50), nullable=False, default='PENDING_DISPATCH')
    stock_part_used_sku = db.Column(db.String(100), nullable=True, default='')
    inven_adjust = db.Column(db.Text, nullable=True, default='')
    stock_bin = db.Column(db.String(100), nullable=True, default='')
    dispatch_doa = db.Column(db.String(20), nullable=True, default='No')
    date_dispatched = db.Column(db.DateTime, nullable=True)
    received_part_sku = db.Column(db.String(100), nullable=True, default='')
    received_ppid = db.Column(db.String(100), nullable=True, default='')
    received_qty = db.Column(db.Integer, nullable=True, default=0)
    received_bin = db.Column(db.String(100), nullable=True, default='')
    received_doa = db.Column(db.String(20), nullable=True, default='No')
    date_replenished = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class InventoryLog(db.Model):
    # ... (InventoryLog model definition remains unchanged) ...
    __tablename__ = 'inventory_log'
    inventory_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    inventory_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    part_sku = db.Column(db.String(100), nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=0)
    log_type = db.Column(db.String(50), nullable=False)
    bin = db.Column(db.String(100), nullable=True, default='')
    notes = db.Column(db.Text, nullable=True, default='')
    related_request_id = db.Column(db.String(36), nullable=True, default='')
    part_acronym = db.Column(db.String(50), nullable=True, default='')

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class User(db.Model, UserMixin): # UserMixin belongs here
    # ... (User model definition remains unchanged) ...
    __tablename__ = 'user' # Optional: Specify table name
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.username}>'


# --- User Loader Callback (Unchanged) ---
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# --- Data Persistence Functions (Unchanged) ---
def _get_dataframes_from_db():
    # ... (Function remains unchanged) ...
    swaps_data = [s.to_dict() for s in SwapRequest.query.all()]
    inventory_data = [l.to_dict() for l in InventoryLog.query.all()]

    if swaps_data:
        df_swaps = pd.DataFrame(swaps_data)
    else:
        columns = [c.name for c in SwapRequest.__table__.columns]
        df_swaps = pd.DataFrame(columns=columns)

    if inventory_data:
        df_inventory_log = pd.DataFrame(inventory_data)
    else:
        columns = [c.name for c in InventoryLog.__table__.columns]
        df_inventory_log = pd.DataFrame(columns=columns)

    if not df_swaps.empty:
        df_swaps['request_id'] = df_swaps['request_id'].astype(str)
        df_swaps['status'] = df_swaps['status'].astype(str).str.upper().str.replace(' ', '_')
        for col in ['date_requested', 'date_dispatched', 'date_replenished']:
             if col in df_swaps.columns:
                 df_swaps[col] = pd.to_datetime(df_swaps[col], errors='coerce').fillna(pd.NaT)

    if not df_inventory_log.empty:
        df_inventory_log['inventory_date'] = pd.to_datetime(df_inventory_log['inventory_date'], errors='coerce').fillna(pd.NaT)
        df_inventory_log['quantity'] = pd.to_numeric(df_inventory_log['quantity'], errors='coerce').fillna(0)
        df_inventory_log = df_inventory_log.fillna('')

    df_swaps = df_swaps.fillna('')
    df_inventory_log = df_inventory_log.fillna('')

    return df_swaps, df_inventory_log


def export_data_to_excel(file_name):
    # ... (Function remains unchanged) ...
    df_swaps, df_inventory_log = _get_dataframes_from_db()
    export_path = os.path.join(DATA_DIR, file_name)
    try:
        with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
            df_swaps.to_excel(writer, sheet_name='Stock_Swaps', index=False)
            df_inventory_log.to_excel(writer, sheet_name='Inventory_Log', index=False)
        return export_path
    except Exception as e:
        print(f"ERROR during export: {e}")
        return None

# --- Utility Functions (Unchanged) ---
def calculate_metrics(df_swaps, df_inventory_log):
    # ... (Function remains unchanged) ...
    current_swaps = df_swaps.copy()
    current_swaps['status'] = current_swaps.get('status', pd.Series([''] * len(current_swaps))).astype(str).str.upper().str.replace(' ', '_')

    pending_dispatch_count = len(current_swaps[current_swaps['status'] == 'PENDING_DISPATCH'])
    pending_receipt_count = len(current_swaps[current_swaps['status'] == 'PENDING_RECEIPT'])
    completed_count = len(current_swaps[current_swaps['status'] == 'COMPLETED'])
    total_pending = pending_dispatch_count + pending_receipt_count

    KNOWN_PARTS_FROM_SWAPS = set(current_swaps.get('part_abbreviation', pd.Series()).dropna().astype(str).str.strip().str.upper().unique())
    KNOWN_PARTS_FROM_LOG = set(df_inventory_log.get('part_acronym', pd.Series()).dropna().astype(str).str.strip().str.upper().unique())
    ALL_KNOWN_PARTS = KNOWN_PARTS_FROM_SWAPS.union(KNOWN_PARTS_FROM_LOG)
    ALL_KNOWN_PARTS = {part for part in ALL_KNOWN_PARTS if part}

    sku_to_acronym = current_swaps[
        (current_swaps.get('stock_part_used_sku', pd.Series()).astype(str) != '') &
        (current_swaps.get('part_abbreviation', pd.Series()).astype(str) != '')
    ].set_index('stock_part_used_sku')['part_abbreviation'].to_dict()

    combined_log = df_inventory_log.copy()

    if 'part_acronym' not in combined_log.columns:
        combined_log['part_acronym'] = ''

    combined_log['quantity'] = pd.to_numeric(combined_log.get('quantity'), errors='coerce').fillna(0)

    def get_acronym(row):
        def find_known_acronym_in_text(text):
            if not isinstance(text, str): return None
            text_upper = text.strip().upper()
            for acronym in ALL_KNOWN_PARTS:
                if text_upper == acronym or f' {acronym} ' in f' {text_upper} ':
                    original_acronym_df = current_swaps[current_swaps.get('part_abbreviation', pd.Series()).astype(str).str.upper() == acronym]
                    if not original_acronym_df.empty:
                        return original_acronym_df.iloc[0]['part_abbreviation']
            return None

        if row.get('part_acronym') and str(row['part_acronym']).strip().upper() in ALL_KNOWN_PARTS:
            return str(row['part_acronym']).strip()
        if row.get('related_request_id'):
            swap_record = current_swaps[current_swaps['request_id'] == row['related_request_id']]
            if not swap_record.empty:
                return swap_record.iloc[0]['part_abbreviation']
        if row.get('log_type') in ['ADJUSTMENT', 'STOCK_IN', 'MANUAL_ADJUSTMENT']:
            acronym_from_sku = find_known_acronym_in_text(row.get('part_sku'))
            if acronym_from_sku: return acronym_from_sku
            acronym_from_notes = find_known_acronym_in_text(row.get('notes'))
            if acronym_from_notes: return acronym_from_notes
        return sku_to_acronym.get(row.get('part_sku'), None)

    combined_log['generic_part'] = combined_log.apply(get_acronym, axis=1)

    usable_log = combined_log[
        (combined_log['generic_part'].notna()) & (combined_log['generic_part'] != '') &
        (~combined_log.get('bin', pd.Series([''] * len(combined_log))).astype(str).str.contains('RMA/DOA', case=False, na=False))
    ].copy()

    part_stock_totals = usable_log.groupby('generic_part')['quantity'].sum()
    part_stock = part_stock_totals.to_dict()
    part_stock_summary = { k: int(v) for k, v in part_stock.items() if int(v) > 0 }

    completed_swaps = current_swaps[current_swaps['status'] == 'COMPLETED'].copy()
    completed_swaps['date_dispatched'] = pd.to_datetime(completed_swaps.get('date_dispatched'), errors='coerce')
    completed_swaps['date_replenished'] = pd.to_datetime(completed_swaps.get('date_replenished'), errors='coerce')
    completed_swaps['time_to_complete'] = (completed_swaps['date_replenished'] - completed_swaps['date_dispatched']).dt.days
    avg_days_to_complete = completed_swaps['time_to_complete'].mean()
    avg_days_to_complete_formatted = 'N/A' if pd.isna(avg_days_to_complete) else f"{avg_days_to_complete:.1f}"

    return {
        'pending_dispatch_count': pending_dispatch_count,
        'pending_receipt_count': pending_receipt_count,
        'completed_count': completed_count,
        'total_pending': total_pending,
        'part_stock_summary': part_stock_summary,
        'avg_days_to_complete': avg_days_to_complete_formatted,
        'all_known_parts': sorted(list(ALL_KNOWN_PARTS))
    }

def get_swap_record(request_id):
    # ... (Function remains unchanged) ...
    swap = SwapRequest.query.get(str(request_id))
    return swap.to_dict() if swap else None

def log_inventory_change(part_sku, quantity, log_type, bin_location, notes, related_request_id=None, part_acronym=''):
    # ... (Function remains unchanged - includes user lookup and adds to notes) ...
    final_acronym = part_acronym
    user_prefix = f"User: {current_user.username if current_user.is_authenticated else 'SYSTEM'} - "

    if not final_acronym and part_sku:
        most_recent_entry_with_acronym = InventoryLog.query.filter(
            InventoryLog.part_sku == part_sku,
            InventoryLog.part_acronym != '', InventoryLog.part_acronym != None,
            InventoryLog.log_type.in_(['STOCK_IN', 'MANUAL_ADJUSTMENT'])
        ).order_by(InventoryLog.inventory_date.desc()).first()
        if most_recent_entry_with_acronym:
            final_acronym = most_recent_entry_with_acronym.part_acronym
        else:
            final_acronym = ''

    new_log_entry = InventoryLog(
        inventory_id=str(uuid.uuid4()),
        inventory_date=datetime.utcnow(),
        part_sku=part_sku,
        quantity=quantity,
        log_type=log_type,
        bin=bin_location,
        notes=user_prefix + notes, # Prepend user info
        part_acronym=final_acronym,
        related_request_id=related_request_id or ''
    )
    db.session.add(new_log_entry)


def get_available_stock_list(requested_acronym=None):
    # ... (Function remains unchanged) ...
    stock_query = db.session.query(
        InventoryLog.part_sku,
        InventoryLog.bin,
        InventoryLog.part_acronym,
        func.sum(InventoryLog.quantity).label('total_quantity')
    ).filter(
        ~InventoryLog.bin.ilike('%RMA/DOA%'),
        InventoryLog.part_sku != '',
        InventoryLog.bin != ''
    )

    if requested_acronym:
        stock_query = stock_query.filter(InventoryLog.part_acronym == requested_acronym)

    stock_query = stock_query.group_by(
        InventoryLog.part_sku,
        InventoryLog.bin,
        InventoryLog.part_acronym
    ).having(
        func.sum(InventoryLog.quantity) > 0
    ).order_by(
        InventoryLog.part_acronym,
        InventoryLog.part_sku
    )

    available_stock = []
    for row in stock_query.all():
        available_stock.append({
            'sku': row.part_sku,
            'bin': row.bin,
            'acronym': row.part_acronym or 'N/A',
            'quantity': int(row.total_quantity)
        })

    return available_stock

def get_all_bins():
    # ... (Function remains unchanged) ...
    bin_query = db.session.query(InventoryLog.bin).filter(
        ~InventoryLog.bin.ilike('%RMA/DOA%'),
        InventoryLog.bin != ''
    ).distinct().order_by(InventoryLog.bin)
    return [row[0] for row in bin_query.all()]


# --- Flask Routes ---

# --- Login/Logout/Register Routes (Unchanged) ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    # ... (Route logic remains unchanged) ...
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user, remember=request.form.get('remember'))
            flash(f'Logged in successfully as {user.username}.', 'success')
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('index'))
        else:
            flash('Login Unsuccessful. Please check username and password', 'error')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    # ... (Route logic remains unchanged) ...
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    # ... (Route logic remains unchanged) ...
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        existing_user = User.query.filter_by(username=username).first()
        if existing_user:
            flash('Username already exists.', 'error')
        else:
            user = User(username=username)
            user.set_password(password)
            db.session.add(user)
            db.session.commit()
            flash('Account created! You can now log in.', 'success')
            return redirect(url_for('login'))
    return render_template('register.html')

# --- PROTECTED ROUTES ---
@app.route('/')
@login_required
def index():
    """MODIFIED: Added pagination"""
    df_swaps_all, df_inventory_log = _get_dataframes_from_db() # Get all for metrics
    filter_status = request.args.get('filter', 'all').upper()
    # --- NEW: Get page number ---
    page = request.args.get('page', 1, type=int)
    # --- END NEW ---

    metrics = calculate_metrics(df_swaps_all, df_inventory_log) # Calculate metrics on all data

    # --- MODIFIED: Base query for active swaps ---
    query = SwapRequest.query.filter(SwapRequest.status.in_(['PENDING_DISPATCH', 'PENDING_RECEIPT']))

    # Apply filter
    if filter_status == 'PENDING_DISPATCH':
        query = query.filter(SwapRequest.status == 'PENDING_DISPATCH')
    elif filter_status == 'PENDING_RECEIPT':
        query = query.filter(SwapRequest.status == 'PENDING_RECEIPT')

    # Apply sorting and pagination
    pagination = query.order_by(SwapRequest.date_requested.asc()).paginate(
        page=page, per_page=ITEMS_PER_PAGE, error_out=False
    )
    swaps_to_display = pagination.items # Get items for the current page
    # --- END MODIFICATION ---

    # Convert SQLAlchemy objects to dicts for template consistency if needed (optional)
    swaps_list = [s.to_dict() for s in swaps_to_display]
    # Clean up dates (same as before, but operate on the list of dicts)
    for swap_dict in swaps_list:
        for col in ['date_requested', 'date_dispatched', 'date_replenished']:
            if col in swap_dict and pd.isna(swap_dict[col]):
                 swap_dict[col] = '' # Replace NaT with empty string


    return render_template(
        'index.html',
        swaps=swaps_list, # Pass the list of dicts
        pagination=pagination, # Pass pagination object
        column_map=COLUMN_MAP,
        pending_dispatch_count=metrics['pending_dispatch_count'],
        pending_receipt_count=metrics['pending_receipt_count'],
        completed_count=metrics['completed_count'],
        pending_count=metrics['total_pending'],
        part_stock_summary=metrics['part_stock_summary'],
        current_filter=filter_status
    )


@app.route('/log_request', methods=['GET', 'POST'])
@login_required
def log_request():
    # ... (Route remains unchanged) ...
    if request.method == 'GET':
        df_swaps, df_inventory_log = _get_dataframes_from_db()
        metrics = calculate_metrics(df_swaps, df_inventory_log)
        all_known_parts = metrics['all_known_parts']
        return render_template('request.html',
                               column_map=COLUMN_MAP,
                               known_parts=all_known_parts,
                               initial_udt='')

    if request.method == 'POST':
        try:
            udt_ticket_wo = request.form['udt_ticket_wo']
            serial_num = request.form['serial_num']
            oem_claim_num = request.form.get('oem_claim_num', '')
            part_list = request.form.getlist('part_abbreviation[]')

            if not part_list or not any(part_list):
                raise ValueError("You must add at least one part to the request.")

            logged_count = 0
            for part in part_list:
                if not part: continue

                new_request = SwapRequest(
                    udt_ticket_wo=udt_ticket_wo,
                    part_abbreviation=part,
                    serial_num=serial_num,
                    oem_claim_num=oem_claim_num,
                    date_requested=datetime.utcnow(),
                    status='PENDING_DISPATCH'
                )
                db.session.add(new_request)
                logged_count += 1

            db.session.commit()
            flash(f"Successfully logged {logged_count} new part request(s) for WO **{udt_ticket_wo}** by {current_user.username}.", 'success') # Added user
            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error logging request: {e}", 'error')
            df_swaps, df_inventory_log = _get_dataframes_from_db()
            metrics = calculate_metrics(df_swaps, df_inventory_log)
            all_known_parts = metrics['all_known_parts']
            return render_template('request.html',
                                   column_map=COLUMN_MAP,
                                   known_parts=all_known_parts,
                                   initial_udt=request.form.get('udt_ticket_wo', ''))

    return redirect(url_for('index'))



@app.route('/edit_request/<request_id>', methods=['GET', 'POST'])
@login_required
def edit_request(request_id):
    # ... (Route remains unchanged) ...
    swap_record = get_swap_record(request_id)
    if not swap_record:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('index'))

    if swap_record['status'] not in ['PENDING_DISPATCH', 'PENDING_RECEIPT']:
        flash('Cannot edit a request that is already processed or completed.', 'error')
        return redirect(url_for('index'))

    df_swaps, df_inventory_log = _get_dataframes_from_db()
    metrics = calculate_metrics(df_swaps, df_inventory_log)
    all_known_parts = metrics['all_known_parts']

    if request.method == 'POST':
        try:
            swap_to_update = SwapRequest.query.get(request_id)
            swap_to_update.udt_ticket_wo = request.form['udt_ticket_wo']
            swap_to_update.part_abbreviation = request.form['part_abbreviation']
            swap_to_update.oem_claim_num = request.form['oem_claim_num']
            swap_to_update.serial_num = request.form['serial_num']
            db.session.commit()
            flash(f"Request {request_id[:8]}... updated successfully by {current_user.username}.", 'success') # Added user
            return redirect(url_for('index'))
        except Exception as e:
            db.session.rollback()
            flash(f"Error updating request: {e}", 'error')
            swap_record = get_swap_record(request_id)

    return render_template('edit_request.html',
                           swap_record=swap_record,
                           column_map=COLUMN_MAP,
                           known_parts=all_known_parts)


@app.route('/dispatch/<request_id>', methods=['GET', 'POST'])
@login_required
def log_dispatch(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    if request.method == 'GET':
        trigger_swap = get_swap_record(request_id)
        if not trigger_swap:
            flash(f"Error: Request ID {request_id} not found.", 'error')
            return redirect(url_for('index'))

        wo = trigger_swap['udt_ticket_wo']
        sn = trigger_swap['serial_num']
        oem = trigger_swap['oem_claim_num']
        requested_acronym = trigger_swap['part_abbreviation'] # For filtering stock

        swaps_to_dispatch_db = SwapRequest.query.filter_by(
            udt_ticket_wo=wo,
            serial_num=sn,
            oem_claim_num=oem,
            status='PENDING_DISPATCH'
        ).order_by(SwapRequest.part_abbreviation).all()

        swaps_to_dispatch = [s.to_dict() for s in swaps_to_dispatch_db]

        if not swaps_to_dispatch:
             flash(f"No parts are pending dispatch for WO: {wo} / SN: {sn} / OEM: {oem or 'N/A'}.", 'info')
             return redirect(url_for('index'))

        available_stock = get_available_stock_list(requested_acronym=requested_acronym)

        shared_details = {
            'udt_ticket_wo': wo,
            'serial_num': sn,
            'oem_claim_num': oem
        }

        return render_template(
            'dispatch.html',
            swaps_to_dispatch=swaps_to_dispatch,
            shared_details=shared_details,
            column_map=COLUMN_MAP,
            available_stock=available_stock,
            requested_acronym=requested_acronym
        )

    if request.method == 'POST':
        try:
            request_ids = request.form.getlist('request_ids[]')
            if not request_ids:
                raise ValueError("No request IDs were submitted.")

            dispatched_count = 0
            wo_num = ""

            for req_id in request_ids:
                swap_to_update = SwapRequest.query.get(req_id)
                if not swap_to_update: continue
                wo_num = swap_to_update.udt_ticket_wo
                stock_selection = request.form.get(f'stock_selection_{req_id}')

                if not stock_selection: continue

                dispatched_sku, stock_bin = stock_selection.split('|')
                inven_adjust = request.form.get(f'inven_adjust_{req_id}', '')

                swap_to_update.status = 'PENDING_RECEIPT'
                swap_to_update.stock_part_used_sku = dispatched_sku
                swap_to_update.inven_adjust = inven_adjust
                swap_to_update.stock_bin = stock_bin
                swap_to_update.dispatch_doa = 'No'
                swap_to_update.date_dispatched = datetime.utcnow()

                log_inventory_change(
                    part_sku=dispatched_sku, quantity=-1, log_type='DISPATCHED', bin_location=stock_bin,
                    notes=f"Dispatched {swap_to_update.part_abbreviation} for UDT WO: {wo_num}. {inven_adjust}",
                    related_request_id=req_id
                )

                dispatched_count += 1

            db.session.commit()

            if dispatched_count > 0:
                flash(f"Successfully dispatched {dispatched_count} part(s) for WO **{wo_num}** by {current_user.username}.", 'success') # Added user
            else:
                flash("No parts were selected for dispatch. All requests remain pending.", 'info')

            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error during batch dispatch: {e}", 'error')
            return redirect(url_for('index'))


@app.route('/edit_dispatch/<request_id>', methods=['GET', 'POST'])
@login_required
def edit_dispatch(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    swap_record = get_swap_record(request_id) # returns dict
    if not swap_record or swap_record['status'] != 'PENDING_RECEIPT':
        flash(f"Error: Cannot edit dispatch for request {request_id[:8]}...", 'error')
        return redirect(url_for('index'))

    requested_acronym = swap_record['part_abbreviation']

    if request.method == 'POST':
        try:
            stock_selection = request.form.get('stock_selection')
            if not stock_selection:
                raise ValueError("No part was selected from stock.")

            new_dispatched_sku, new_dispatched_bin = stock_selection.split('|')
            new_dispatch_doa = request.form.get('dispatch_doa', 'No')
            new_inven_adjust = request.form.get('inven_adjust', '')

            original_sku = swap_record['stock_part_used_sku']
            original_bin = swap_record['stock_bin']
            original_doa = swap_record.get('dispatch_doa', 'No')

            if (original_sku == new_dispatched_sku and
                original_bin == new_dispatched_bin and
                original_doa == new_dispatch_doa):
                flash("No changes detected. Edit cancelled.", 'info')
                return redirect(url_for('index'))

            # --- Process Inventory ---
            if original_doa == 'No':
                log_inventory_change(
                    part_sku=original_sku, quantity=1, log_type='ADJUSTMENT', bin_location=original_bin,
                    notes=f"DISPATCH EDIT REVERSAL: Returning original part for request {request_id[:8]}...",
                    related_request_id=request_id
                )
            if new_dispatch_doa == 'No':
                log_inventory_change(
                    part_sku=new_dispatched_sku, quantity=-1, log_type='DISPATCHED', bin_location=new_dispatched_bin,
                    notes=f"CORRECTED DISPATCH: New part for request {request_id[:8]}... {new_inven_adjust}",
                    related_request_id=request_id
                )
                flash_msg = f"Dispatch details updated by {current_user.username}. Inventory adjusted." # Added user
            else:
                 flash_msg = f"Dispatch details updated by {current_user.username}. New part marked DOA." # Added user

            if original_doa == 'No' and new_dispatch_doa == 'Yes':
                 flash_msg = f"Dispatch details updated by {current_user.username}. Part marked DOA. Original part returned." # Added user
            elif original_doa == 'Yes' and new_dispatch_doa == 'No':
                 flash_msg = f"Dispatch details updated by {current_user.username}. Part UN-marked DOA. New part removed." # Added user

            # --- Update swap record ---
            swap_to_update = SwapRequest.query.get(request_id)
            swap_to_update.stock_part_used_sku = new_dispatched_sku
            swap_to_update.stock_bin = new_dispatched_bin
            swap_to_update.dispatch_doa = new_dispatch_doa
            swap_to_update.inven_adjust = new_inven_adjust

            db.session.commit()
            flash(flash_msg, 'success')
            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"An unexpected error occurred during dispatch edit: {e}", 'error')

    # GET request
    available_stock = get_available_stock_list(requested_acronym=requested_acronym)

    return render_template('dispatch.html',
                           title='Edit Dispatched Part', action='edit',
                           swap_record=swap_record, available_stock=available_stock,
                           request_id=request_id, column_map=COLUMN_MAP,
                           requested_acronym=requested_acronym)


@app.route('/receive/<request_id>', methods=['GET', 'POST'])
@login_required
def log_receive(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    if request.method == 'GET':
        trigger_swap = get_swap_record(request_id)
        if not trigger_swap:
            flash(f"Error: Request ID {request_id} not found.", 'error')
            return redirect(url_for('index'))

        wo = trigger_swap['udt_ticket_wo']
        sn = trigger_swap['serial_num']
        oem = trigger_swap['oem_claim_num']

        swaps_to_receive_db = SwapRequest.query.filter_by(
            udt_ticket_wo=wo,
            serial_num=sn,
            oem_claim_num=oem,
            status='PENDING_RECEIPT'
        ).order_by(SwapRequest.part_abbreviation).all()

        swaps_to_receive = [s.to_dict() for s in swaps_to_receive_db]

        if not swaps_to_receive:
             flash(f"No parts are pending receipt for WO: {wo} / SN: {sn} / OEM: {oem or 'N/A'}.", 'info')
             return redirect(url_for('index'))

        sku_query = db.session.query(InventoryLog.part_sku).distinct().all()
        all_skus = sorted([sku[0] for sku in sku_query if sku[0]])
        all_bins = get_all_bins()

        shared_details = {
            'udt_ticket_wo': wo,
            'serial_num': sn,
            'oem_claim_num': oem
        }

        return render_template(
            'receive.html',
            swaps_to_receive=swaps_to_receive,
            shared_details=shared_details,
            column_map=COLUMN_MAP,
            all_skus=all_skus,
            all_bins=all_bins
        )

    if request.method == 'POST':
        try:
            request_ids = request.form.getlist('request_ids[]')
            if not request_ids:
                raise ValueError("No request IDs were submitted.")

            received_count = 0
            wo_num = ""

            for req_id in request_ids:
                swap_to_update = SwapRequest.query.get(req_id)
                if not swap_to_update: continue
                wo_num = swap_to_update.udt_ticket_wo
                received_ppid = request.form.get(f'received_ppid_{req_id}')

                if not received_ppid: continue

                received_sku = request.form.get(f'received_part_sku_{req_id}')
                received_qty = int(request.form.get(f'received_qty_{req_id}', 1))
                received_bin = request.form.get(f'received_bin_{req_id}')
                received_doa = request.form.get(f'received_doa_{req_id}', 'No')

                swap_to_update.status = 'COMPLETED'
                swap_to_update.received_part_sku = received_sku
                swap_to_update.received_ppid = received_ppid
                swap_to_update.received_qty = received_qty
                swap_to_update.received_bin = received_bin
                swap_to_update.received_doa = received_doa
                swap_to_update.date_replenished = datetime.utcnow()

                log_bin = 'RMA/DOA' if received_doa == 'Yes' else received_bin
                log_notes = f"Received failed part (PPID: {received_ppid}) from UDT WO: {wo_num}."

                log_acronym = ''
                if received_doa == 'No':
                    log_notes += " Replenished to stock."
                    log_acronym = swap_to_update.part_abbreviation
                else:
                    log_notes += " **Marked as DOA - NOT added to usable stock.**"

                log_inventory_change(
                    part_sku=received_sku, quantity=received_qty, log_type='STOCK_IN', bin_location=log_bin,
                    notes=log_notes, related_request_id=req_id, part_acronym=log_acronym
                )

                received_count += 1

            db.session.commit()

            if received_count > 0:
                flash(f"Successfully received {received_count} part(s) for WO **{wo_num}** by {current_user.username}.", 'success') # Added user
            else:
                flash("No parts were filled out for receipt. All requests remain pending.", 'info')

            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error during batch receipt: {e}", 'error')
            return redirect(url_for('index'))


@app.route('/completed_swaps')
@login_required
def completed_swaps():
    """MODIFIED: Added pagination"""
    # --- NEW: Get page number ---
    page = request.args.get('page', 1, type=int)
    # --- END NEW ---

    # --- MODIFIED: Base query ---
    query = SwapRequest.query.filter_by(status='COMPLETED')

    # --- MODIFIED: Apply pagination ---
    pagination = query.order_by(SwapRequest.date_replenished.desc()).paginate(
        page=page, per_page=ITEMS_PER_PAGE, error_out=False
    )
    completed_swaps_db = pagination.items # Get items for current page
    # --- END MODIFICATION ---

    if not completed_swaps_db:
        return render_template(
            'completed_swaps.html',
            swaps=[],
            pagination=pagination, # Pass pagination object even if empty
            column_map=COLUMN_MAP,
            avg_days_to_complete="N/A",
            completed_count=0
        )

    # Convert to DataFrame for calculations (still useful for average)
    completed_df = pd.DataFrame([s.to_dict() for s in SwapRequest.query.filter_by(status='COMPLETED').all()]) # Need all for avg calc

    # --- Calculate Average (Unchanged) ---
    avg_days_to_complete = "N/A"
    if not completed_df.empty:
        completed_df['date_dispatched'] = pd.to_datetime(completed_df['date_dispatched'], errors='coerce')
        completed_df['date_replenished'] = pd.to_datetime(completed_df['date_replenished'], errors='coerce')
        completed_df['days_to_complete'] = (completed_df['date_replenished'] - completed_df['date_dispatched']).dt.days
        avg_days = completed_df['days_to_complete'].mean()
        avg_days_to_complete = f"{avg_days:.1f}" if pd.notna(avg_days) else "N/A"

    # Convert page items to dicts for template
    completed_swaps_list = [s.to_dict() for s in completed_swaps_db]
     # Clean up dates (operate on the list of dicts)
    for swap_dict in completed_swaps_list:
        for col in ['date_requested', 'date_dispatched', 'date_replenished']:
            if col in swap_dict and pd.isna(swap_dict[col]):
                 swap_dict[col] = '' # Replace NaT with empty string


    return render_template(
        'completed_swaps.html',
        swaps=completed_swaps_list, # Pass page items
        pagination=pagination,       # Pass pagination object
        column_map=COLUMN_MAP,
        avg_days_to_complete=avg_days_to_complete,
        completed_count=pagination.total # Use total from pagination
    )


@app.route('/inventory_management', methods=['GET', 'POST'])
@login_required
def inventory_management():
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    if request.method == 'POST':
        try:
            part_acronym = request.form['part_acronym'].strip()
            part_sku = request.form['part_sku'].strip()
            quantity = int(request.form['quantity'])
            bin_location = request.form.get('bin', 'ADJUSTMENT_BIN').strip().upper()
            notes = request.form.get('notes', '').strip()

            if quantity == 0:
                raise ValueError("Quantity cannot be zero.")

            log_inventory_change(
                part_sku=part_sku, quantity=quantity, log_type='MANUAL_ADJUSTMENT', bin_location=bin_location,
                notes=f"Manual Adjustment: {notes}", related_request_id=None, part_acronym=part_acronym
            )
            db.session.commit()
            action = "added to" if quantity > 0 else "removed from"
            flash(f"Manual adjustment logged by {current_user.username}: {abs(quantity)} x **{part_acronym}** {action} inventory.", 'success') # Added user
            return redirect(url_for('inventory_management'))

        except (ValueError, KeyError) as e:
            db.session.rollback()
            flash(f"Error in form: {e}. Quantity must be a non-zero number.", 'error')
        except Exception as e:
            db.session.rollback()
            flash(f"An unexpected error occurred: {e}", 'error')

    # GET Request
    df_swaps, df_inventory_log = _get_dataframes_from_db()
    metrics = calculate_metrics(df_swaps, df_inventory_log)
    inventory_detail_df = df_inventory_log.copy()

    inventory_detail_df['quantity'] = pd.to_numeric(inventory_detail_df['quantity'], errors='coerce').fillna(0)
    usable_inventory = inventory_detail_df[
        (~inventory_detail_df['bin'].astype(str).str.contains('RMA/DOA', case=False, na=False)) &
        (inventory_detail_df['part_sku'].astype(str) != '')
    ].copy()

    stock_to_display = []
    if not usable_inventory.empty:
        stock_by_sku_bin = usable_inventory.groupby(['part_sku', 'bin'])['quantity'].sum().reset_index()
        authoritative_entries = usable_inventory[
            (usable_inventory['log_type'].isin(['MANUAL_ADJUSTMENT', 'STOCK_IN'])) &
            (usable_inventory['part_acronym'].astype(str) != '')
        ].copy()
        if not authoritative_entries.empty:
            authoritative_entries = authoritative_entries.sort_values('inventory_date', ascending=False)
            acronym_map_df = authoritative_entries.drop_duplicates(subset=['part_sku', 'bin'], keep='first')[['part_sku', 'bin', 'part_acronym']]
            stock_by_sku_bin = pd.merge(stock_by_sku_bin, acronym_map_df, on=['part_sku', 'bin'], how='left')
        else:
             stock_by_sku_bin['part_acronym'] = 'N/A'

        stock_by_sku_bin['part_acronym'] = stock_by_sku_bin['part_acronym'].fillna('N/A')
        stock_to_display_df = stock_by_sku_bin[stock_by_sku_bin['quantity'] > 0]
        stock_to_display = stock_to_display_df.to_dict('records')

    # --- MODIFIED: Paginate Log History ---
    page = request.args.get('log_page', 1, type=int) # Use different param name to avoid conflict
    log_pagination = InventoryLog.query.order_by(InventoryLog.inventory_date.desc()).paginate(
        page=page, per_page=ITEMS_PER_PAGE, error_out=False
    )
    log_entries = [l.to_dict() for l in log_pagination.items]
    # --- END MODIFICATION ---
    
    for entry in log_entries:
        if 'inventory_date' in entry and pd.isna(entry['inventory_date']):
            entry['inventory_date'] = ''

    all_bins = get_all_bins()
    all_known_parts = metrics['all_known_parts']

    acronym_sku_map = defaultdict(set)
    sku_query_for_map = db.session.query(InventoryLog.part_acronym, InventoryLog.part_sku).filter(
        InventoryLog.part_acronym != '', InventoryLog.part_acronym != None,
        InventoryLog.part_sku != '', InventoryLog.part_sku != None
    ).distinct().all()
    for acronym, sku in sku_query_for_map:
        acronym_sku_map[acronym].add(sku)
    acronym_sku_map_serializable = {k: sorted(list(v)) for k, v in acronym_sku_map.items()}

    return render_template(
        'inventory_management.html',
        all_bins=all_bins,
        known_parts=all_known_parts,
        acronym_sku_map=acronym_sku_map_serializable,
        column_map=COLUMN_MAP,
        part_stock_summary=metrics['part_stock_summary'],
        stock_detail=stock_to_display,
        log_entries=log_entries,
        log_pagination=log_pagination # Pass log pagination object
    )


@app.route('/flag_doa/<request_id>')
@login_required
def flag_doa(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    try:
        swap = SwapRequest.query.get(str(request_id))
        if swap:
            flash_msg = f"Request {request_id[:8]}... flagged as DOA by {current_user.username}."
            if swap.dispatch_doa == 'No' and swap.stock_part_used_sku and swap.stock_bin:
                log_inventory_change(
                    part_sku=swap.stock_part_used_sku, quantity=1, log_type='ADJUSTMENT', bin_location=swap.stock_bin,
                    notes=f"FLAG DOA: Returning part {swap.stock_part_used_sku} to stock.",
                    related_request_id=request_id
                )
                flash_msg += f" Part returned to stock bin {swap.stock_bin}."
            elif swap.dispatch_doa == 'Yes':
                 flash_msg = f"Request {request_id[:8]}... part was already flagged as DOA. No change."

            swap.dispatch_doa = 'Yes'
            db.session.commit()
            flash(flash_msg, 'warning')
        else:
            flash(f"Error: Request {request_id[:8]}... not found.", 'error')
    except Exception as e:
        db.session.rollback()
        flash(f"Error flagging DOA: {e}", 'error')
    return redirect(url_for('index'))


@app.route('/unflag_doa/<request_id>')
@login_required
def unflag_doa(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    try:
        swap = SwapRequest.query.get(str(request_id))
        if swap:
            flash_msg = f"Request {request_id[:8]}... un-flagged as DOA by {current_user.username}."
            if swap.dispatch_doa == 'Yes' and swap.stock_part_used_sku and swap.stock_bin:
                log_inventory_change(
                    part_sku=swap.stock_part_used_sku, quantity=-1, log_type='ADJUSTMENT', bin_location=swap.stock_bin,
                    notes=f"UN-FLAG DOA: Removing part {swap.stock_part_used_sku} from stock.",
                    related_request_id=request_id
                )
                flash_msg += f" Part removed from stock bin {swap.stock_bin}."
            elif swap.dispatch_doa == 'No':
                 flash_msg = f"Request {request_id[:8]}... part was already NOT flagged as DOA. No change."

            swap.dispatch_doa = 'No'
            db.session.commit()
            flash(flash_msg, 'success')
        else:
            flash(f"Error: Request {request_id[:8]}... not found.", 'error')
    except Exception as e:
        db.session.rollback()
        flash(f"Error un-flagging DOA: {e}", 'error')
    return redirect(url_for('index'))


@app.route('/export')
@login_required
def export_data():
    # ... (Route logic unchanged) ...
    file_name = f"Swap_Tracker_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    file_path = export_data_to_excel(file_name=file_name)
    if file_path:
        return send_from_directory(directory=DATA_DIR, path=file_name, as_attachment=True)
    else:
        flash('ERROR: Failed to generate the export file.', 'error')
        return redirect(url_for('index'))


@app.route('/cancel_request/<request_id>')
@login_required
def cancel_request(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    swap_record = get_swap_record(request_id)
    if not swap_record:
        flash(f"Error: Request ID {request_id[:8]}... not found.", 'error')
        return redirect(url_for('index'))

    current_status = swap_record['status']
    if current_status not in ['PENDING_DISPATCH', 'PENDING_RECEIPT']:
        flash(f"Error: Cannot cancel request {request_id[:8]}... Status is: **{current_status}**.", 'error')
        return redirect(url_for('index'))

    try:
        flash_message = ""
        if current_status == 'PENDING_RECEIPT' and swap_record.get('dispatch_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=swap_record['stock_part_used_sku'], quantity=1, log_type='ADJUSTMENT', bin_location=swap_record['stock_bin'],
                notes=f"CANCEL PENDING_RECEIPT: Reversed stock out for request {request_id[:8]}...",
                related_request_id=request_id
            )
            flash_message = f"Inventory adjusted (part returned to bin {swap_record['stock_bin']}). "

        swap_to_delete = SwapRequest.query.get(str(request_id))
        if swap_to_delete:
            db.session.delete(swap_to_delete)
            db.session.commit()
            flash(f"**Request Cancelled by {current_user.username}!** {flash_message}Swap request {request_id[:8]}... deleted.", 'success') # Added user
        else:
             flash(f"Error: Request ID {request_id[:8]}... not found for deletion.", 'error')

    except Exception as e:
        db.session.rollback()
        flash(f"An unexpected error occurred while cancelling: {e}", 'error')

    return redirect(url_for('index'))


@app.route('/reopen/<request_id>')
@login_required
def reopen_for_claim(request_id):
    # ... (Route logic unchanged, calls log_inventory_change which adds user) ...
    reopen_reason = request.args.get('reason', None)
    original_swap = get_swap_record(request_id)
    if not original_swap:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('completed_swaps'))

    try:
        if original_swap.get('received_part_sku') and original_swap.get('received_qty') and original_swap.get('received_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=original_swap['received_part_sku'], quantity=-abs(int(original_swap['received_qty'])), log_type='ADJUSTMENT',
                bin_location=original_swap['received_bin'],
                notes=f"STOCK REVERSAL (REOPEN): Reversing received part from request {request_id[:8]}...",
                related_request_id=request_id, part_acronym=original_swap['part_abbreviation']
            )
        if original_swap.get('stock_part_used_sku') and original_swap.get('dispatch_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=original_swap['stock_part_used_sku'], quantity=1, log_type='ADJUSTMENT',
                bin_location=original_swap['stock_bin'],
                notes=f"STOCK REVERSAL (REOPEN): Returning dispatched part for request {request_id[:8]}...",
                related_request_id=request_id
            )

        swap_to_update = SwapRequest.query.get(request_id)
        swap_to_update.status = 'PENDING_DISPATCH'

        if reopen_reason == 'DOA_RECEIVED_FAILURE':
            swap_to_update.received_doa = 'Yes - Post Install'
            flash_message = f"Request {request_id[:8]}... reopened by {current_user.username}. Received part flagged DOA." # Added user
        else:
            swap_to_update.stock_part_used_sku, swap_to_update.inven_adjust, swap_to_update.stock_bin = '', '', ''
            swap_to_update.dispatch_doa, swap_to_update.date_dispatched = 'No', None
            swap_to_update.received_part_sku, swap_to_update.received_ppid, swap_to_update.received_qty = '', '', 0
            swap_to_update.received_bin, swap_to_update.received_doa, swap_to_update.date_replenished = '', 'No', None
            flash_message = f"Request {request_id[:8]}... reopened for dispatch by {current_user.username}. Fields reset." # Added user

        db.session.commit()
        flash(flash_message, 'success')

    except Exception as e:
        db.session.rollback()
        flash(f"Error reopening request: {e}", 'error')

    return redirect(url_for('index'))


# --- Run Block ---
if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    with app.app_context():
        db.create_all()
        if not User.query.first():
            print("Creating default admin user...")
            default_user = User(username='admin')
            default_user.set_password('password') # CHANGE THIS PASSWORD
            db.session.add(default_user)
            db.session.commit()
            print("Default admin user created with username 'admin' and password 'password'. Please change the password.")
    print("--- Attempting to start Flask development server... ---")
    app.run(debug=True, host='0.0.0.0', port=5001)