import os
import uuid
from datetime import datetime
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory
from flask_sqlalchemy import SQLAlchemy

# --- App Initialization ---
app = Flask(__name__, template_folder='templates')
app.secret_key = 'super_secret_key_for_flash_messages' 
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

import os

# --- Database Configuration (Secure) ---
# Pulls credentials from the server's environment variables
# Uses your values as a "fallback" if the variables aren't set
DB_DRIVER = 'ODBC+Driver+17+for+SQL+Server'
DB_HOST = os.environ.get('DB_HOST', 'localhost,50567')
DB_NAME = os.environ.get('DB_NAME', 'warehouseSC')
DB_USER = os.environ.get('DB_USER', 'dev')
DB_PASS = os.environ.get('DB_PASS') # No fallback for password in production

if not DB_PASS:
    print("Warning: DB_PASS environment variable is not set. Using fallback for local dev.")
    DB_PASS = 'UDTonline2026!!'
    
# The connection string
app.config['SQLALCHEMY_DATABASE_URI'] = f'mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}?driver={DB_DRIVER}'

# The database connection string need to revise this code after testing

db = SQLAlchemy(app)

# --- Global Configuration (Unchanged) ---
COLUMN_MAP = {
    # ... (Your entire COLUMN_MAP remains unchanged) ...
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

KNOWN_PARTS = [
    'BC', 'BT', 'HT', 'KBB', 'LCD', 'LCD-BC', 'LCDC',
]

# --- Database Models (Replaces Excel Schema) ---

class SwapRequest(db.Model):
    """ Model for the Stock Swaps (replaces stock_swap_tracker.xlsx) """
    __tablename__ = 'swap_request'
    
    request_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    udt_ticket_wo = db.Column(db.String(100), nullable=False)
    part_abbreviation = db.Column(db.String(50), nullable=False)
    serial_num = db.Column(db.String(100), nullable=False)
    oem_claim_num = db.Column(db.String(100), nullable=True, default='')
    date_requested = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    status = db.Column(db.String(50), nullable=False, default='PENDING_DISPATCH')
    
    # Dispatch Fields
    stock_part_used_sku = db.Column(db.String(100), nullable=True, default='')
    inven_adjust = db.Column(db.Text, nullable=True, default='')
    stock_bin = db.Column(db.String(100), nullable=True, default='')
    dispatch_doa = db.Column(db.String(20), nullable=True, default='No')
    date_dispatched = db.Column(db.DateTime, nullable=True)
    
    # Receipt Fields
    received_part_sku = db.Column(db.String(100), nullable=True, default='')
    received_ppid = db.Column(db.String(100), nullable=True, default='')
    received_qty = db.Column(db.Integer, nullable=True, default=0)
    received_bin = db.Column(db.String(100), nullable=True, default='')
    received_doa = db.Column(db.String(20), nullable=True, default='No')
    date_replenished = db.Column(db.DateTime, nullable=True)

    def to_dict(self):
        """Converts model object to a dictionary, compatible with old templates."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class InventoryLog(db.Model):
    """ Model for the Inventory Log (replaces inventory_log.xlsx) """
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
        """Converts model object to a dictionary, compatible with old templates."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# --- Data Persistence Functions (REFACTORED) ---

# load_data() and save_data() are NO LONGER NEEDED. 
# The database handles this automatically.

def _get_dataframes_from_db():
    """
    Queries all data from the database and returns two Pandas DataFrames
    with the same cleanup logic that load_data() used to perform.
    This is used for functions that rely on Pandas for complex calculations
    (like calculate_metrics, completed_swaps, inventory_management).
    """
    
    # 1. Query all data from DB
    swaps_data = [s.to_dict() for s in SwapRequest.query.all()]
    inventory_data = [l.to_dict() for l in InventoryLog.query.all()]

    # 2. Create DataFrames
    if swaps_data:
        df_swaps = pd.DataFrame(swaps_data)
    else:
        # Create empty DF with correct columns if DB is empty
        columns = [c.name for c in SwapRequest.__table__.columns]
        df_swaps = pd.DataFrame(columns=columns)
        
    if inventory_data:
        df_inventory_log = pd.DataFrame(inventory_data)
    else:
        # Create empty DF with correct columns if DB is empty
        columns = [c.name for c in InventoryLog.__table__.columns]
        df_inventory_log = pd.DataFrame(columns=columns)

    # 3. Perform the *same cleanup* as the old load_data() function
    # This ensures calculate_metrics() gets data in the format it expects.
    
    # --- Swaps Cleanup ---
    if not df_swaps.empty:
        # A. Ensure primary IDs are strings
        df_swaps['request_id'] = df_swaps['request_id'].astype(str)
        
        # B. Ensure status is standardized
        df_swaps['status'] = df_swaps['status'].astype(str).str.upper().str.replace(' ', '_')

        # C. Convert all relevant date columns
        for col in ['date_requested', 'date_dispatched', 'date_replenished']:
             if col in df_swaps.columns:
                 df_swaps[col] = pd.to_datetime(df_swaps[col], errors='coerce').fillna(pd.NaT)
    
    # --- Inventory Log Cleanup ---
    if not df_inventory_log.empty:
        # A. DATE CONVERSION
        df_inventory_log['inventory_date'] = pd.to_datetime(df_inventory_log['inventory_date'], errors='coerce').fillna(pd.NaT)
        
        # B. NUMERIC CONVERSION
        df_inventory_log['quantity'] = pd.to_numeric(df_inventory_log['quantity'], errors='coerce').fillna(0)

        # C. Fill NaNs
        df_inventory_log = df_inventory_log.fillna('')
        
    # Replace any lingering NaNs (especially from empty DFs)
    df_swaps = df_swaps.fillna('')
    df_inventory_log = df_inventory_log.fillna('')

    return df_swaps, df_inventory_log


def export_data_to_excel(file_name):
    """
    Loads all data from the database, converts to DataFrames, and exports to Excel.
    (This is the one place Pandas is still required for I/O).
    """
    # 1. Get the DataFrames from the DB helper
    df_swaps, df_inventory_log = _get_dataframes_from_db()
    
    export_path = os.path.join(DATA_DIR, file_name)
    
    try:
        # 2. Write to Excel (same as before)
        with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
            df_swaps.to_excel(writer, sheet_name='Stock_Swaps', index=False)
            df_inventory_log.to_excel(writer, sheet_name='Inventory_Log', index=False)
        
        return export_path
    except Exception as e:
        print(f"ERROR during export: {e}")
        return None  

# --- Utility Functions ---

# calculate_metrics() IS UNCHANGED.
# It relies on the _get_dataframes_from_db() helper to provide
# the exact same DataFrame format it's always used.
def calculate_metrics(df_swaps, df_inventory_log):
    """Calculates dashboard metrics, available part stock, and time to completion."""
    
    # 1. Dashboard Metrics
    current_swaps = df_swaps.copy()
    current_swaps['status'] = current_swaps.get('status', pd.Series([''] * len(current_swaps))).astype(str).str.upper().str.replace(' ', '_')
    
    pending_dispatch_count = len(current_swaps[current_swaps['status'] == 'PENDING_DISPATCH'])
    pending_receipt_count = len(current_swaps[current_swaps['status'] == 'PENDING_RECEIPT'])
    completed_count = len(current_swaps[current_swaps['status'] == 'COMPLETED'])
    total_pending = pending_dispatch_count + pending_receipt_count

    # --- CRITICAL ADDITION: Define KNOWN_PARTS and prepare log ---
    KNOWN_PARTS = set(current_swaps.get('part_abbreviation', pd.Series()).dropna().astype(str).str.strip().str.upper().unique())
    
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
            if not isinstance(text, str):
                return None
            text_upper = text.strip().upper()
            for acronym in KNOWN_PARTS:
                if text_upper == acronym or f' {acronym} ' in f' {text_upper} ':
                    original_acronym_df = current_swaps[current_swaps.get('part_abbreviation', pd.Series()).astype(str).str.upper() == acronym]
                    if not original_acronym_df.empty:
                        return original_acronym_df.iloc[0]['part_abbreviation']
            return None

        if row.get('part_acronym') and str(row['part_acronym']).strip().upper() in KNOWN_PARTS:
            return str(row['part_acronym']).strip() 
            
        if row.get('related_request_id'):
            swap_record = current_swaps[current_swaps['request_id'] == row['related_request_id']]
            if not swap_record.empty:
                return swap_record.iloc[0]['part_abbreviation']
        
        if row.get('log_type') in ['ADJUSTMENT', 'STOCK_IN']:
            
            acronym_from_sku = find_known_acronym_in_text(row.get('part_sku'))
            if acronym_from_sku:
                return acronym_from_sku

            acronym_from_notes = find_known_acronym_in_text(row.get('notes'))
            if acronym_from_notes:
                return acronym_from_notes
        
        return sku_to_acronym.get(row.get('part_sku'), None) 

    combined_log['generic_part'] = combined_log.apply(get_acronym, axis=1)
    
    # --- Group and Sum Stock by Generic Acronym (Existing Logic) ---
    usable_log = combined_log[
        (combined_log['generic_part'].notna()) & 
        (~combined_log.get('bin', pd.Series([''] * len(combined_log))).astype(str).str.contains('RMA/DOA', case=False, na=False))
    ].copy()

    part_stock_totals = usable_log.groupby('generic_part')['quantity'].sum()
    
    part_stock = part_stock_totals.to_dict()
    part_stock_summary = {
        k: int(v) 
        for k, v in part_stock.items() 
        if int(v) > 0 
    }

    # --- Time to Completion Metric ---
    completed_swaps = current_swaps[current_swaps['status'] == 'COMPLETED'].copy()

    completed_swaps['date_dispatched'] = pd.to_datetime(completed_swaps.get('date_dispatched'), errors='coerce')
    completed_swaps['date_replenished'] = pd.to_datetime(completed_swaps.get('date_replenished'), errors='coerce')

    completed_swaps['time_to_complete'] = (completed_swaps['date_replenished'] - completed_swaps['date_dispatched']).dt.days

    avg_days_to_complete = completed_swaps['time_to_complete'].mean()
    
    if pd.isna(avg_days_to_complete):
        avg_days_to_complete_formatted = 'N/A'
    else:
        avg_days_to_complete_formatted = f"{avg_days_to_complete:.1f}"

    return {
        'pending_dispatch_count': pending_dispatch_count,
        'pending_receipt_count': pending_receipt_count,
        'completed_count': completed_count,
        'total_pending': total_pending,
        'part_stock_summary': part_stock_summary,
        'avg_days_to_complete': avg_days_to_complete_formatted
    }

def get_swap_record(request_id):
    """Retrieves a single swap record by request_id and returns it as a dict."""
    swap = SwapRequest.query.get(str(request_id))
    return swap.to_dict() if swap else None

def log_inventory_change(part_sku, quantity, log_type, bin_location, notes, related_request_id=None, part_acronym=''):
    """
    Adds a new entry to the inventory log in the DB session.
    NOTE: This function NO LONGER calls save_data(). The route must call db.session.commit().
    """
    new_log_entry = InventoryLog(
        inventory_id=str(uuid.uuid4()),
        inventory_date=datetime.utcnow(),
        part_sku=part_sku, 
        quantity=quantity,
        log_type=log_type,
        bin=bin_location,
        notes=notes,
        part_acronym=part_acronym,
        related_request_id=related_request_id or ''
    )
    # Add to the session, but don't commit. The route will commit.
    db.session.add(new_log_entry)


# --- Flask Routes (REFACTORED) ---

@app.route('/')
def index():
    """Dashboard view showing pending requests and metrics."""
    # 1. Get DataFrames from DB
    df_swaps, df_inventory_log = _get_dataframes_from_db()

    filter_status = request.args.get('filter', 'all').upper() 
    
    # 2. Calculate metrics (Pandas logic is unchanged)
    metrics = calculate_metrics(df_swaps, df_inventory_log)
    
    # 3. Filter data for display (Pandas logic is unchanged)
    active_swaps_df = df_swaps[df_swaps['status'].isin(['PENDING_DISPATCH', 'PENDING_RECEIPT'])].copy()
    
    if filter_status == 'PENDING_DISPATCH':
        df_display = active_swaps_df[active_swaps_df['status'] == 'PENDING_DISPATCH']
    elif filter_status == 'PENDING_RECEIPT':
        df_display = active_swaps_df[active_swaps_df['status'] == 'PENDING_RECEIPT']
    else:
        df_display = active_swaps_df
        
    df_display = df_display.sort_values(by='date_requested', ascending=True)

    # 4. Clean up dates for display (Pandas logic is unchanged)
    date_cols = ['date_requested', 'date_dispatched', 'date_replenished']
    for col in date_cols:
        if col in df_display.columns:
            # Convert NaT (Not a Time) to empty string for rendering
            df_display[col] = df_display[col].apply(lambda x: '' if pd.isna(x) else x)
            
    # 5. Convert final DataFrame to dicts for template
    swaps_to_display = df_display.to_dict('records')

    return render_template(
        'index.html',
        swaps=swaps_to_display, 
        column_map=COLUMN_MAP,
        pending_dispatch_count=metrics['pending_dispatch_count'],
        pending_receipt_count=metrics['pending_receipt_count'],
        completed_count=metrics['completed_count'],
        pending_count=metrics['total_pending'],
        part_stock_summary=metrics['part_stock_summary'],
        current_filter=filter_status
    )

@app.route('/log_request', methods=['GET', 'POST'])
def log_request():
    """Allows logging a new swap request."""
    if request.method == 'POST':
        try:
            # 1. Create new SwapRequest object
            new_request = SwapRequest(
                # request_id is generated by default
                udt_ticket_wo=request.form['udt_ticket_wo'],
                part_abbreviation=request.form['part_abbreviation'],
                serial_num=request.form['serial_num'],
                oem_claim_num=request.form.get('oem_claim_num', ''),
                date_requested=datetime.utcnow(), 
                status='PENDING_DISPATCH'
                # All other fields use model defaults ('', None, 'No', etc.)
            )
            
            # 2. Add to session and commit
            db.session.add(new_request)
            db.session.commit()
            
            flash(f"New Swap Request (ID: {new_request.request_id[:8]}...) for Part **{new_request.part_abbreviation}** has been logged.", 'success')
            return redirect(url_for('index'))
        except Exception as e:
            db.session.rollback()
            flash(f"Error logging request: {e}", 'error')

    return render_template('request.html', column_map=COLUMN_MAP, known_parts=KNOWN_PARTS)

@app.route('/edit_request/<request_id>', methods=['GET', 'POST'])
def edit_request(request_id):
    
    # 1. Fetch the existing record (for GET and pre-check)
    swap_record = get_swap_record(request_id) # This returns a dict

    if not swap_record:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('index'))

    if swap_record['status'] not in ['PENDING_DISPATCH', 'PENDING_RECEIPT']:
        flash('Cannot edit a request that is already processed or completed.', 'error')
        return redirect(url_for('index'))

    if request.method == 'POST':
        try:
            # 1. Get the object to update
            swap_to_update = SwapRequest.query.get(request_id)
            
            # 2. Update its fields
            swap_to_update.udt_ticket_wo = request.form['udt_ticket_wo']
            swap_to_update.part_abbreviation = request.form['part_abbreviation']
            swap_to_update.oem_claim_num = request.form['oem_claim_num']
            swap_to_update.serial_num = request.form['serial_num']
            
            # 3. Commit the change
            db.session.commit() 
            
            flash(f"Request {request_id[:8]}... updated successfully!", 'success')
            return redirect(url_for('index'))
        except Exception as e:
            db.session.rollback()
            flash(f"Error updating request: {e}", 'error')
            # Re-fetch the record in case of error
            swap_record = get_swap_record(request_id)

    # 3. Render the template
    return render_template('edit_request.html', 
                           swap_record=swap_record, 
                           column_map=COLUMN_MAP,
                           known_parts=KNOWN_PARTS) # Added known_parts

@app.route('/dispatch/<request_id>', methods=['GET', 'POST'])
def log_dispatch(request_id):
    """Allows logging the dispatch of a replacement part."""
    swap = get_swap_record(request_id) # returns a dict
    if not swap:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('index'))

    if swap['status'] != 'PENDING_DISPATCH':
        flash(f"Error: Request {request_id} is not pending dispatch.", 'error')
        return redirect(url_for('index'))

    if request.method == 'POST':
        try:
            dispatched_sku = request.form['stock_part_used_sku'] 
            stock_bin = request.form['stock_bin']
            dispatch_doa = request.form.get('dispatch_doa', 'No') 
            inven_adjust = request.form.get('inven_adjust', '') 
            
            # 1. Get the DB object to update
            swap_to_update = SwapRequest.query.get(request_id)
            
            # 2. Update its fields
            swap_to_update.status = 'PENDING_RECEIPT'
            swap_to_update.stock_part_used_sku = dispatched_sku
            swap_to_update.inven_adjust = inven_adjust
            swap_to_update.stock_bin = stock_bin
            swap_to_update.dispatch_doa = dispatch_doa
            swap_to_update.date_dispatched = datetime.utcnow()
            
            # 3. Log the inventory change (if not DOA)
            if dispatch_doa == 'No':
                log_inventory_change(
                    part_sku=dispatched_sku, 
                    quantity=-1, 
                    log_type='DISPATCHED',
                    bin_location=stock_bin,
                    notes=f"Dispatched replacement part for UDT WO: {swap['udt_ticket_wo']}. {inven_adjust}",
                    related_request_id=request_id
                )
                flash_message = f"Part {dispatched_sku} dispatched successfully for request {request_id[:8]}... and **removed from stock**."
            else:
                flash_message = f"Part {dispatched_sku} dispatched for request {request_id[:8]}..., but was **marked as DOA and was NOT removed from stock**."

            # 4. Commit all changes (swap update + inventory log)
            db.session.commit()
            
            flash(flash_message, 'success')
            return redirect(url_for('index'))
        
        except Exception as e:
            db.session.rollback()
            flash(f"Error during dispatch: {e}", 'error')

    # GET request
    # Need to get part stock for the dropdown
    df_swaps, df_inventory_log = _get_dataframes_from_db()
    metrics = calculate_metrics(df_swaps, df_inventory_log)
    available_parts = list(metrics['part_stock_summary'].keys())
    
    return render_template(
        'dispatch.html', 
        swap_record=swap, # Pass the dict
        column_map=COLUMN_MAP, 
        available_parts=available_parts
    )

@app.route('/edit_dispatch/<request_id>', methods=['GET', 'POST'])
def edit_dispatch(request_id):
    """Allows editing of dispatched SKU and Bin for PENDING_RECEIPT swaps."""
    swap_record = get_swap_record(request_id) # returns dict
    if not swap_record or swap_record['status'] != 'PENDING_RECEIPT':
        flash(f"Error: Cannot edit dispatch for request {request_id[:8]}...", 'error')
        return redirect(url_for('index'))

    # Get available SKUs from inventory log
    sku_query = db.session.query(InventoryLog.part_sku).distinct().all()
    available_skus = [sku[0] for sku in sku_query if sku[0]]
    
    if request.method == 'POST':
        try:
            new_dispatched_sku = request.form['stock_part_used_sku']
            new_dispatched_bin = request.form['stock_bin']
            
            original_sku = swap_record['stock_part_used_sku']
            
            # 1. Reverse the original dispatch log entry
            log_inventory_change(
                part_sku=original_sku,
                quantity=1, # Revert the -1
                log_type='ADJUSTMENT',
                bin_location='EDIT_REVERSE',
                notes=f"DISPATCH EDIT REVERSAL: Revert original dispatch for request {request_id[:8]}...",
                related_request_id=request_id
            )
            
            # 2. Log the new, corrected dispatch entry
            log_inventory_change(
                part_sku=new_dispatched_sku,
                quantity=-1,
                log_type='DISPATCHED',
                bin_location=new_dispatched_bin,
                notes=f"CORRECTED DISPATCH: New part for request {request_id[:8]}...",
                related_request_id=request_id
            )

            # 3. Update the swap request object
            swap_to_update = SwapRequest.query.get(request_id)
            swap_to_update.stock_part_used_sku = new_dispatched_sku
            swap_to_update.stock_bin = new_dispatched_bin
            
            # 4. Commit all changes
            db.session.commit()
            
            flash(f"Dispatch details for request {request_id[:8]}... updated to SKU: {new_dispatched_sku}", 'success')
            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"An unexpected error occurred during dispatch edit: {e}", 'error')

    # GET request
    return render_template('dispatch.html', 
                           title='Edit Dispatched Part', 
                           action='edit', 
                           swap_record=swap_record, 
                           known_parts=KNOWN_PARTS, 
                           available_skus=available_skus, 
                           request_id=request_id,
                           column_map=COLUMN_MAP)


@app.route('/receive/<request_id>', methods=['GET', 'POST'])
def log_receive(request_id):
    """Allows logging the receipt of the failed/old part."""
    swap = get_swap_record(request_id) # returns dict
    if not swap:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('index'))

    if swap['status'] != 'PENDING_RECEIPT':
        flash(f"Error: Request {request_id} is not pending receipt.", 'error')
        return redirect(url_for('index'))

    if request.method == 'POST':
        try:
            received_sku = request.form['received_part_sku']
            received_ppid = request.form['received_ppid']
            received_qty = int(request.form['received_qty']) 
            received_bin = request.form['received_bin']
            received_doa = request.form.get('received_doa', 'No')
            
            # 1. Get DB object to update
            swap_to_update = SwapRequest.query.get(request_id)

            # 2. Update its fields
            swap_to_update.status = 'COMPLETED'
            swap_to_update.received_part_sku = received_sku
            swap_to_update.received_ppid = received_ppid
            swap_to_update.received_qty = received_qty
            swap_to_update.received_bin = received_bin
            swap_to_update.received_doa = received_doa
            swap_to_update.date_replenished = datetime.utcnow()
            
            # 3. Log the Inventory IN transaction
            log_bin = 'RMA/DOA' if received_doa == 'Yes' else received_bin
            log_notes = f"Received failed part (PPID: {received_ppid}) from UDT WO: {swap['udt_ticket_wo']}."
            
            if received_doa == 'No':
                log_notes += " Replenished to stock."
                flash_message = f"Failed part receipt logged for {request_id[:8]}... and **{received_qty} x {received_sku} added to stock**."
            else:
                log_notes += " **Marked as DOA - NOT added to usable stock.**"
                flash_message = f"Failed part receipt logged for {request_id[:8]}.... **Part marked as DOA.**"

            log_inventory_change(
                part_sku=received_sku, 
                quantity=received_qty, 
                log_type='STOCK_IN',
                bin_location=log_bin,
                notes=log_notes,
                related_request_id=request_id
            )

            # 4. Commit all changes
            db.session.commit()
            
            flash(flash_message, 'success')
            return redirect(url_for('index'))

        except Exception as e:
            db.session.rollback()
            flash(f"Error during receipt: {e}", 'error')

    # GET request: Prepare dict for template
    entry = {
        'request_id': swap['request_id'],
        'udt_ticket_wo': swap['udt_ticket_wo'],
        'part_abbreviation': swap['part_abbreviation'],
        'serial_num': swap['serial_num'],
        'stock_part_used_sku': swap.get('stock_part_used_sku', ''), # Dispatched SKU
        'received_part_pn': swap.get('received_part_sku', swap.get('stock_part_used_sku', '')), # Suggest dispatched SKU
        'received_ppid': swap.get('received_ppid', ''),
        'received_qty': swap.get('received_qty', 1),
        'received_bin': swap.get('received_bin', ''),
        'received_doa': swap.get('received_doa', 'No'),
    }
    
    return render_template('receive.html', entry=entry, column_map=COLUMN_MAP, known_parts=KNOWN_PARTS)


@app.route('/completed_swaps')
def completed_swaps():
    """Displays a list of all completed swap requests."""
    # 1. Query only completed swaps
    completed_swaps_db = SwapRequest.query.filter_by(status='COMPLETED').order_by(SwapRequest.date_replenished.desc()).all()
    
    if not completed_swaps_db:
        # Handle empty case
        return render_template(
            'completed_swaps.html',
            swaps=[],
            column_map=COLUMN_MAP,
            avg_days_to_complete="N/A",
            completed_count=0
        )

    # 2. Convert to DataFrame for Pandas calculations (days_to_complete)
    completed_df = pd.DataFrame([s.to_dict() for s in completed_swaps_db])

    # 3. Perform Pandas operations (same as before)
    completed_df['date_dispatched'] = pd.to_datetime(completed_df['date_dispatched'], errors='coerce')
    completed_df['date_replenished'] = pd.to_datetime(completed_df['date_replenished'], errors='coerce')
    completed_df['days_to_complete'] = (completed_df['date_replenished'] - completed_df['date_dispatched']).dt.days

    avg_days_to_complete = completed_df['days_to_complete'].mean()
    avg_days_str = f"{avg_days_to_complete:.1f}" if pd.notna(avg_days_to_complete) else "N/A"
    
    # 4. Clean up NaT for display and convert to dicts
    completed_df = completed_df.fillna('')
    completed_df['date_requested'] = completed_df['date_requested'].apply(lambda x: '' if pd.isna(x) else x)
    completed_df['date_dispatched'] = completed_df['date_dispatched'].apply(lambda x: '' if pd.isna(x) else x)
    completed_df['date_replenished'] = completed_df['date_replenished'].apply(lambda x: '' if pd.isna(x) else x)

    completed_swaps_list = completed_df.to_dict('records')

    return render_template(
        'completed_swaps.html',
        swaps=completed_swaps_list,
        column_map=COLUMN_MAP,
        avg_days_to_complete=avg_days_str,
        completed_count=len(completed_swaps_list)
    )


@app.route('/inventory_management', methods=['GET', 'POST'])
def inventory_management():
    """Allows manual inventory adjustments and views the detailed inventory."""

    # --- POST Request (Log New Adjustment) ---
    if request.method == 'POST':
        try:
            part_acronym = request.form['part_acronym'].strip()
            part_sku = request.form['part_sku'].strip()
            quantity = int(request.form['quantity'])
            bin_location = request.form.get('bin', 'ADJUSTMENT_BIN').strip().upper()  
            notes = request.form.get('notes', '').strip()

            if quantity == 0:
                raise ValueError("Quantity cannot be zero.")

            # 1. Log the change (this adds to session)
            log_inventory_change(
                part_sku=part_sku,
                quantity=quantity,
                log_type='MANUAL_ADJUSTMENT',
                bin_location=bin_location,
                notes=f"Manual Adjustment: {notes}",
                related_request_id=None,
                part_acronym=part_acronym
            )
            
            # 2. Commit the change
            db.session.commit()

            action = "added to" if quantity > 0 else "removed from"
            flash(f"Manual adjustment logged: {abs(quantity)} x **{part_acronym}** (SKU: {part_sku}) {action} inventory.", 'success')
            return redirect(url_for('inventory_management'))

        except (ValueError, KeyError) as e:
            db.session.rollback()
            flash(f"Error in form: {e}. Quantity must be a non-zero number.", 'error')
        except Exception as e:
            db.session.rollback()
            flash(f"An unexpected error occurred: {e}", 'error')

    # --- GET Request (Display Current Stock and Log) ---
    
    # 1. Get DataFrames for complex Pandas calculations
    df_swaps, df_inventory_log = _get_dataframes_from_db()

    # 2. Calculate overall metrics
    metrics = calculate_metrics(df_swaps, df_inventory_log)

    # 3. Detailed Inventory Breakdown (Pandas logic is unchanged)
    inventory_detail_df = df_inventory_log.copy() # We use the cleaned DF
    
    if 'part_acronym' not in inventory_detail_df.columns:
        inventory_detail_df['part_acronym'] = 'SKU_ONLY'
        
    # Ensure quantity is numeric (already done by helper, but good to double check)
    inventory_detail_df['quantity'] = pd.to_numeric(inventory_detail_df['quantity'], errors='coerce').fillna(0)
    
    usable_inventory = inventory_detail_df[
        (~inventory_detail_df['bin'].astype(str).str.contains('RMA/DOA', case=False, na=False)) &
        (inventory_detail_df['part_sku'].astype(str) != '')
    ].copy()
    
    grouping_keys = ['part_acronym', 'part_sku', 'bin']
    stock_by_sku_bin = usable_inventory.groupby(grouping_keys)['quantity'].sum().reset_index()
    stock_to_display = stock_by_sku_bin[stock_by_sku_bin['quantity'] > 0].to_dict('records')
    
    # 4. Raw Log Entries (Audit Trail) - Query DB directly for this
    log_entries_db = InventoryLog.query.order_by(InventoryLog.inventory_date.desc()).limit(50).all()
    log_entries = [l.to_dict() for l in log_entries_db]
    
    # 5. Data for form dropdowns - Query DB directly
    sku_query = db.session.query(InventoryLog.part_sku).distinct().all()
    all_skus = sorted([sku[0] for sku in sku_query if sku[0]])

    return render_template(
        'inventory_management.html',
        all_skus=all_skus,
        known_parts=KNOWN_PARTS, 
        column_map=COLUMN_MAP,
        part_stock_summary=metrics['part_stock_summary'], # Generic Summary
        stock_detail=stock_to_display,                   # Detailed Stock
        log_entries=log_entries                          # Audit Trail
    )

@app.route('/flag_doa/<request_id>')
def flag_doa(request_id):
    """Flags the DISPATCHED part for a specific request as DOA."""
    try:
        swap = SwapRequest.query.get(str(request_id))
        if swap:
            swap.dispatch_doa = 'Yes'
            db.session.commit()
            flash(f"Request {request_id[:8]}... dispatched part flagged as DOA.", 'warning')
        else:
            flash(f"Error: Request {request_id[:8]}... not found.", 'error')
    except Exception as e:
        db.session.rollback()
        flash(f"Error flagging DOA: {e}", 'error')
    return redirect(url_for('index'))

@app.route('/unflag_doa/<request_id>')
def unflag_doa(request_id):
    """Reverts the dispatch_doa flag from 'Yes' back to 'No'."""
    try:
        swap = SwapRequest.query.get(str(request_id))
        if swap:
            swap.dispatch_doa = 'No'
            db.session.commit()
            flash(f"Request {request_id[:8]}... dispatched part un-flagged as DOA.", 'success')
        else:
            flash(f"Error: Request {request_id[:8]}... not found.", 'error')
    except Exception as e:
        db.session.rollback()
        flash(f"Error un-flagging DOA: {e}", 'error')
    return redirect(url_for('index'))


@app.route('/export')
def export_data():
    """Triggers the data export and initiates a file download."""
    file_name = f"Swap_Tracker_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    file_path = export_data_to_excel(file_name=file_name)
    
    if file_path:
        return send_from_directory(
            directory=DATA_DIR, 
            path=file_name,
            as_attachment=True
        )
    else:
        flash('ERROR: Failed to generate the export file.', 'error')
        return redirect(url_for('index'))

@app.route('/cancel_request/<request_id>') 
def cancel_request(request_id):
    """Deletes a swap request. If PENDING_RECEIPT, it reverses the dispatch inventory log."""
    swap_record = get_swap_record(request_id) # returns dict

    if not swap_record:
        flash(f"Error: Request ID {request_id[:8]}... not found.", 'error')
        return redirect(url_for('index'))

    current_status = swap_record['status']

    if current_status not in ['PENDING_DISPATCH', 'PENDING_RECEIPT']:
        flash(f"Error: Cannot cancel request {request_id[:8]}... Status is: **{current_status}**.", 'error')
        return redirect(url_for('index'))

    try:
        flash_message = ""
        # 1. Reverse Stock Out if PENDING_RECEIPT and not DOA
        if current_status == 'PENDING_RECEIPT' and swap_record.get('dispatch_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=swap_record['stock_part_used_sku'],
                quantity=1, # Revert the stock out
                log_type='ADJUSTMENT',
                bin_location='CANCEL_REVERSE',
                notes=f"CANCEL PENDING_RECEIPT: Reversed stock out for request {request_id[:8]}...",
                related_request_id=request_id
            )
            flash_message = "Inventory adjusted (stock returned). "
        
        # 2. Delete the swap record
        swap_to_delete = SwapRequest.query.get(str(request_id))
        if swap_to_delete:
            db.session.delete(swap_to_delete)
            
            # 3. Commit all changes (log and deletion)
            db.session.commit()
            
            flash(f"**Request Cancelled!** {flash_message}Swap request {request_id[:8]}... has been deleted.", 'success')
        else:
             flash(f"Error: Request ID {request_id[:8]}... not found for deletion.", 'error')
        
    except Exception as e:
        db.session.rollback()
        flash(f"An unexpected error occurred while cancelling: {e}", 'error')

    return redirect(url_for('index'))

@app.route('/reopen/<request_id>')
def reopen_for_claim(request_id):
    """Reverts a completed swap back to PENDING_DISPATCH state."""
    reopen_reason = request.args.get('reason', None)
    
    original_swap = get_swap_record(request_id) # returns dict
    if not original_swap:
        flash(f"Error: Request ID {request_id} not found.", 'error')
        return redirect(url_for('completed_swaps'))
        
    try:
        # 1. Reverse the stock IN (the failed part)
        if original_swap.get('received_part_sku') and original_swap.get('received_qty') and original_swap.get('received_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=original_swap['received_part_sku'], 
                quantity=-abs(int(original_swap['received_qty'])), 
                log_type='ADJUSTMENT', 
                bin_location='REVERSE_RECEIPT', 
                notes=f"STOCK REVERSAL: Reopened request {request_id[:8]}...",
                related_request_id=request_id
            )

        # 2. Reverse the stock OUT (the replacement part)
        if original_swap.get('stock_part_used_sku') and original_swap.get('dispatch_doa', 'No') == 'No':
            log_inventory_change(
                part_sku=original_swap['stock_part_used_sku'], 
                quantity=1, 
                log_type='ADJUSTMENT', 
                bin_location='REVERSE_DISPATCH', 
                notes=f"STOCK REVERSAL: Reopened request {request_id[:8]}...",
                related_request_id=request_id
            )

        # 3. Get the DB object to update
        swap_to_update = SwapRequest.query.get(request_id)
        
        # 4. Update its fields
        swap_to_update.status = 'PENDING_DISPATCH'

        if reopen_reason == 'DOA_RECEIVED_FAILURE':
            # Special case: Flag received part as DOA, keep dispatch data
            swap_to_update.received_doa = 'Yes - Post Install'
            flash_message = f"Request {request_id[:8]}... reopened. **Original RECEIVED part flagged as DOA (Post-Install Failure).** Inventory adjusted."
        else:
            # Standard Reopen: clear all dispatch/receive fields
            swap_to_update.stock_part_used_sku = ''
            swap_to_update.inven_adjust = ''
            swap_to_update.stock_bin = ''
            swap_to_update.dispatch_doa = 'No'
            swap_to_update.date_dispatched = None
            swap_to_update.received_part_sku = ''
            swap_to_update.received_ppid = ''
            swap_to_update.received_qty = 0
            swap_to_update.received_bin = ''
            swap_to_update.received_doa = 'No'
            swap_to_update.date_replenished = None
            flash_message = f"Request {request_id[:8]}... successfully reopened for dispatch. Inventory adjusted."
        
        # 5. Commit all changes
        db.session.commit()
        flash(flash_message, 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(f"Error reopening request: {e}", 'error')

    return redirect(url_for('index'))


# --- Run Block ---

if __name__ == '__main__':
    # This will create the 'data' directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # This will connect to the DB and create the tables if they don't exist
    with app.app_context():
        db.create_all()
        
    print("--- Attempting to start Flask development server... ---")
    app.run(debug=True, host='0.0.0.0', port=5001)
