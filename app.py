"""
Pixlcat Coffee SF Ferry Building - Square POS API Middleware
Mirrors Toast API architecture at toast-api-1.onrender.com
Deployed on Render.com

Endpoints:
  GET  /health              - Health check
  GET  /sales/yesterday     - Yesterday's sales summary
  GET  /sales/<date>        - Sales for specific date (YYYY-MM-DD)
  GET  /sales/range?start=YYYY-MM-DD&end=YYYY-MM-DD - Date range summary
  GET  /labor/yesterday     - Yesterday's labor/timecard data
  GET  /labor/<date>        - Labor for specific date
  GET  /schedule/today      - Today's published scheduled shifts
  GET  /schedule/tomorrow   - Tomorrow's published scheduled shifts
  GET  /schedule/<date>     - Scheduled shifts for specific date (YYYY-MM-DD)
  GET  /catalog             - Full menu catalog
  POST /slack/events        - Slack Events API for interactive schedule queries
"""

import os
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, send_file
import httpx

# ── Config ──────────────────────────────────────────────────────────────────

SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
SQUARE_LOCATION_ID = os.getenv("SQUARE_LOCATION_ID")
SQUARE_API_VERSION = os.getenv("SQUARE_API_VERSION", "2025-05-21")
SQUARE_BASE_URL = "https://connect.squareup.com/v2"
TIMEZONE = ZoneInfo("America/Los_Angeles")  # SF Ferry Building timezone

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Square API Client ───────────────────────────────────────────────────────

def square_headers():
    return {
        "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Square-Version": SQUARE_API_VERSION,
    }


def square_request(method, endpoint, json_body=None, params=None):
    """Make a request to the Square API with error handling."""
    url = f"{SQUARE_BASE_URL}/{endpoint}"
    with httpx.Client(timeout=60) as client:
        resp = client.request(
            method, url, headers=square_headers(), json=json_body, params=params
        )
        if resp.status_code != 200:
            logger.error(f"Square API error: {resp.status_code} - {resp.text}")
            resp.raise_for_status()
        return resp.json()


# ── Date Helpers ────────────────────────────────────────────────────────────

def get_business_date(date_str=None):
    """Parse date string or return yesterday's date in SF Ferry Building timezone."""
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    now = datetime.now(TIMEZONE)
    return (now - timedelta(days=1)).date()


def date_to_rfc3339_range(date_obj):
    """Convert a date to RFC3339 start/end timestamps for Square API."""
    start = datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=TIMEZONE)
    end = start + timedelta(days=1)
    return start.isoformat(), end.isoformat()


def get_today_sf():
    """Get today's date in SF Ferry Building timezone."""
    return datetime.now(TIMEZONE).date()


def get_tomorrow_sf():
    """Get tomorrow's date in SF Ferry Building timezone."""
    return (datetime.now(TIMEZONE) + timedelta(days=1)).date()


# ── Order Fetching (handles pagination) ─────────────────────────────────────

def fetch_all_orders(start_at, end_at, location_id=None):
    """Fetch all COMPLETED orders in a time range, handling pagination."""
    loc_id = location_id or SQUARE_LOCATION_ID
    all_orders = []
    cursor = None

    while True:
        body = {
            "location_ids": [loc_id],
            "query": {
                "filter": {
                    "date_time_filter": {
                        "closed_at": {
                            "start_at": start_at,
                            "end_at": end_at,
                        }
                    },
                    "state_filter": {"states": ["COMPLETED"]},
                },
                "sort": {"sort_field": "CLOSED_AT", "sort_order": "ASC"},
            },
            "limit": 500,
        }
        if cursor:
            body["cursor"] = cursor

        data = square_request("POST", "orders/search", json_body=body)
        orders = data.get("orders", [])
        all_orders.extend(orders)
        cursor = data.get("cursor")
        if not cursor:
            break

    return all_orders


# ── Catalog Fetching ────────────────────────────────────────────────────────

def fetch_catalog():
    """Fetch the full catalog to map item variation IDs to names."""
    all_objects = []
    cursor = None

    while True:
        params = {"types": "ITEM,ITEM_VARIATION,CATEGORY,MODIFIER,MODIFIER_LIST"}
        if cursor:
            params["cursor"] = cursor

        data = square_request("GET", "catalog/list", params=params)
        objects = data.get("objects", [])
        all_objects.extend(objects)
        cursor = data.get("cursor")
        if not cursor:
            break

    return all_objects


def build_catalog_map(catalog_objects):
    """Build lookup maps from catalog objects."""
    items = {}
    variations = {}
    categories = {}

    for obj in catalog_objects:
        obj_type = obj.get("type")
        obj_id = obj.get("id")

        if obj_type == "CATEGORY":
            cat_data = obj.get("category_data", {})
            categories[obj_id] = cat_data.get("name", "Unknown")

        elif obj_type == "ITEM":
            item_data = obj.get("item_data", {})
            item_name = item_data.get("name", "Unknown")
            category_id = item_data.get("reporting_category", {}).get("id")
            items[obj_id] = {
                "name": item_name,
                "category_id": category_id,
            }

            for var in item_data.get("variations", []):
                var_id = var.get("id")
                var_data = var.get("item_variation_data", {})
                var_name = var_data.get("name", "")

                display_name = item_name
                if var_name and var_name != "Regular" and var_name != item_name:
                    display_name = f"{item_name} - {var_name}"

                variations[var_id] = {
                    "name": display_name,
                    "item_name": item_name,
                    "category_id": category_id,
                }

    for var_id, var_info in variations.items():
        cat_id = var_info.get("category_id")
        var_info["category"] = categories.get(cat_id, "Uncategorized") if cat_id else "Uncategorized"

    return variations, categories


# ── Sales Metrics Parsing ───────────────────────────────────────────────────

def parse_sales_metrics(orders, catalog_map=None):
    """Parse Square orders into Pixlcat-compatible metrics."""
    if not orders:
        return {
            "total_orders": 0, "net_sales": 0, "gross_sales": 0,
            "total_tax": 0, "total_discount": 0, "total_tip": 0,
            "total_service_charges": 0, "avg_check": 0, "items_sold": 0,
            "items": {}, "categories": {}, "hourly": {},
            "mochi": {
                "orders_with_mochi": 0, "total_mochi_items": 0,
                "mochi_revenue": 0, "attachment_rate": 0, "flavors": {},
            },
        }

    total_orders = len(orders)
    gross_sales = 0
    net_sales = 0
    total_tax = 0
    total_discount = 0
    total_tip = 0
    total_service_charges = 0
    items_sold = 0
    item_breakdown = {}
    category_breakdown = {}
    hourly_breakdown = {}

    mochi_orders = set()
    mochi_item_count = 0
    mochi_revenue = 0
    mochi_flavors = {}

    for order in orders:
        money = order.get("total_money", {})
        tax_money = order.get("total_tax_money", {})
        discount_money = order.get("total_discount_money", {})
        tip_money = order.get("total_tip_money", {})
        service_money = order.get("total_service_charge_money", {})

        order_total = money.get("amount", 0) / 100
        order_tax = tax_money.get("amount", 0) / 100
        order_discount = discount_money.get("amount", 0) / 100
        order_tip = tip_money.get("amount", 0) / 100
        order_service = service_money.get("amount", 0) / 100

        gross_sales += order_total
        total_tax += order_tax
        total_discount += order_discount
        total_tip += order_tip
        total_service_charges += order_service

        order_net = order_total - order_tax - order_tip

        closed_at = order.get("closed_at", "")
        hour_key = None
        if closed_at:
            try:
                dt = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))
                local_dt = dt.astimezone(TIMEZONE)
                hour_key = local_dt.strftime("%H:00")
                if hour_key not in hourly_breakdown:
                    hourly_breakdown[hour_key] = {"orders": 0, "revenue": 0, "items": 0}
                hourly_breakdown[hour_key]["orders"] += 1
                hourly_breakdown[hour_key]["revenue"] += order_net
            except (ValueError, TypeError):
                pass

        order_has_mochi = False
        for item in order.get("line_items", []):
            qty = int(item.get("quantity", "1"))
            items_sold += qty

            catalog_obj_id = item.get("catalog_object_id", "")
            item_name = item.get("name", "Unknown Item")
            category = "Uncategorized"

            if catalog_map and catalog_obj_id in catalog_map:
                mapped = catalog_map[catalog_obj_id]
                item_name = mapped["name"]
                category = mapped.get("category", "Uncategorized")

            item_total = item.get("total_money", {}).get("amount", 0) / 100

            if item_name not in item_breakdown:
                item_breakdown[item_name] = {"count": 0, "revenue": 0}
            item_breakdown[item_name]["count"] += qty
            item_breakdown[item_name]["revenue"] += item_total

            if category not in category_breakdown:
                category_breakdown[category] = {"count": 0, "revenue": 0}
            category_breakdown[category]["count"] += qty
            category_breakdown[category]["revenue"] += item_total

            if closed_at and hour_key and hour_key in hourly_breakdown:
                hourly_breakdown[hour_key]["items"] += qty

            name_lower = item_name.lower()
            cat_lower = category.lower() if category else ""
            if "mochi" in name_lower or "butter mochi" in name_lower or "butter mochi" in cat_lower:
                order_has_mochi = True
                mochi_item_count += qty
                mochi_revenue += item_total

                is_box = "box" in name_lower or "pack" in name_lower
                modifiers = item.get("modifiers", [])

                if is_box and modifiers:
                    for mod in modifiers:
                        mod_name = mod.get("name", "")
                        mod_catalog_id = mod.get("catalog_object_id", "")
                        if catalog_map and mod_catalog_id in catalog_map:
                            mod_name = catalog_map[mod_catalog_id].get("item_name", mod_name)
                        if not mod_name:
                            mod_name = "Unknown"
                        mod_clean = normalize_mochi_flavor(mod_name)
                        skip_words = ["tax", "doordash", "merchant", "additive"]
                        if any(sw in mod_clean.lower() for sw in skip_words):
                            continue
                        if mod_clean not in mochi_flavors:
                            mochi_flavors[mod_clean] = {"count": 0, "revenue": 0}
                        mochi_flavors[mod_clean]["count"] += qty

                    box_label = extract_mochi_flavor(item_name)
                    if box_label not in mochi_flavors:
                        mochi_flavors[box_label] = {"count": 0, "revenue": 0}
                    mochi_flavors[box_label]["revenue"] += item_total
                else:
                    flavor = normalize_mochi_flavor(extract_mochi_flavor(item_name))
                    if flavor not in mochi_flavors:
                        mochi_flavors[flavor] = {"count": 0, "revenue": 0}
                    mochi_flavors[flavor]["count"] += qty
                    mochi_flavors[flavor]["revenue"] += item_total

        if order_has_mochi:
            mochi_orders.add(order.get("id"))

    net_sales = gross_sales - total_tax - total_tip
    avg_check = net_sales / total_orders if total_orders else 0
    mochi_attachment = (len(mochi_orders) / total_orders * 100) if total_orders else 0

    sorted_items = dict(sorted(item_breakdown.items(), key=lambda x: x[1]["revenue"], reverse=True))
    sorted_categories = dict(sorted(category_breakdown.items(), key=lambda x: x[1]["revenue"], reverse=True))
    sorted_hourly = dict(sorted(hourly_breakdown.items()))
    sorted_flavors = dict(sorted(mochi_flavors.items(), key=lambda x: x[1]["count"], reverse=True))

    return {
        "total_orders": total_orders,
        "net_sales": round(net_sales, 2),
        "gross_sales": round(gross_sales, 2),
        "total_tax": round(total_tax, 2),
        "total_discount": round(total_discount, 2),
        "total_tip": round(total_tip, 2),
        "total_service_charges": round(total_service_charges, 2),
        "avg_check": round(avg_check, 2),
        "items_sold": items_sold,
        "items": sorted_items,
        "categories": sorted_categories,
        "hourly": sorted_hourly,
        "mochi": {
            "orders_with_mochi": len(mochi_orders),
            "total_mochi_items": mochi_item_count,
            "mochi_revenue": round(mochi_revenue, 2),
            "attachment_rate": round(mochi_attachment, 1),
            "flavors": sorted_flavors,
        },
    }


def normalize_mochi_flavor(name):
    """Normalize mochi flavor names to prevent duplicates."""
    clean = name.strip().title()
    flavor_map = {
        "Smore": "S'more", "S'More": "S'more", "S'more": "S'more",
        "Smores": "S'more", "S'Mores": "S'more",
        "Black Sesame": "Black Sesame", "Blacksesame": "Black Sesame",
        "Ube": "Ube", "Matcha": "Matcha", "Classic": "Classic",
        "Chocolate": "Chocolate", "Breakfast": "Breakfast",
        "Super Bowl": "Super Bowl",
    }
    return flavor_map.get(clean, clean)


def extract_mochi_flavor(item_name):
    """Extract the flavor from a mochi item name."""
    name = item_name.lower()
    if "box" in name or "pack" in name:
        if "3" in name:
            return "3-Pack Box"
        elif "6" in name:
            return "6-Pack Box"
        elif "12" in name:
            return "12-Pack Box"
        return "Box"
    for suffix in ["butter mochi", "mochi"]:
        name = name.replace(suffix, "").strip()
    name = name.strip(" -\u2013\u2014")
    if not name:
        return "Classic"
    return name.title()


# ── Labor/Timecard Parsing ──────────────────────────────────────────────────

def fetch_timecards(start_at, end_at, location_id=None):
    """Fetch timecards (shifts) for a date range."""
    loc_id = location_id or SQUARE_LOCATION_ID
    body = {
        "query": {
            "filter": {
                "location_ids": [loc_id],
                "start": {"start_at": start_at, "end_at": end_at},
                "status": "CLOSED",
            }
        },
        "limit": 200,
    }
    try:
        data = square_request("POST", "labor/timecards/search", json_body=body)
        return data.get("timecards", [])
    except httpx.HTTPStatusError:
        try:
            data = square_request("POST", "labor/shifts/search", json_body=body)
            return data.get("shifts", [])
        except httpx.HTTPStatusError as e:
            logger.warning(f"Labor API not available: {e}")
            return []


def fetch_team_members():
    """Fetch team member details for name mapping."""
    try:
        body = {"limit": 200}
        data = square_request("POST", "team-members/search", json_body=body)
        members = {}
        for m in data.get("team_members", []):
            members[m["id"]] = {
                "name": f"{m.get('given_name', '')} {m.get('family_name', '')}".strip(),
                "status": m.get("status", ""),
            }
        return members
    except Exception as e:
        logger.warning(f"Could not fetch team members: {e}")
        return {}


def parse_labor_metrics(timecards, team_members=None):
    """Parse timecards into labor metrics."""
    if not timecards:
        return {"total_shifts": 0, "total_hours": 0, "total_labor_cost": 0, "team": {}}

    total_hours = 0
    total_cost = 0
    team_breakdown = {}

    for tc in timecards:
        start = tc.get("start_at", "")
        end = tc.get("end_at", "")
        member_id = tc.get("team_member_id", "unknown")
        wage = tc.get("wage", {})
        hourly_rate = wage.get("hourly_rate", {}).get("amount", 0) / 100
        job_title = wage.get("job_title", "Team Member")

        if start and end:
            try:
                start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                hours = (end_dt - start_dt).total_seconds() / 3600

                for brk in tc.get("breaks", []):
                    brk_start = brk.get("start_at", "")
                    brk_end = brk.get("end_at", "")
                    if brk_start and brk_end:
                        bs = datetime.fromisoformat(brk_start.replace("Z", "+00:00"))
                        be = datetime.fromisoformat(brk_end.replace("Z", "+00:00"))
                        hours -= (be - bs).total_seconds() / 3600

                hours = max(0, hours)
                shift_cost = hours * hourly_rate
                total_hours += hours
                total_cost += shift_cost

                member_name = member_id
                if team_members and member_id in team_members:
                    member_name = team_members[member_id]["name"]

                if member_name not in team_breakdown:
                    team_breakdown[member_name] = {
                        "hours": 0, "cost": 0, "job_title": job_title,
                        "hourly_rate": hourly_rate, "shifts": 0,
                    }
                team_breakdown[member_name]["hours"] += round(hours, 2)
                team_breakdown[member_name]["cost"] += round(shift_cost, 2)
                team_breakdown[member_name]["shifts"] += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing timecard times: {e}")
                continue

    return {
        "total_shifts": len(timecards),
        "total_hours": round(total_hours, 2),
        "total_labor_cost": round(total_cost, 2),
        "avg_hourly_cost": round(total_cost / total_hours, 2) if total_hours else 0,
        "team": team_breakdown,
    }


# ── SPLH Calculator ─────────────────────────────────────────────────────────

def calculate_splh(sales_metrics, labor_metrics):
    """Calculate Sales Per Labor Hour."""
    net_sales = sales_metrics.get("net_sales", 0)
    total_hours = labor_metrics.get("total_hours", 0)
    total_labor = labor_metrics.get("total_labor_cost", 0)
    splh = net_sales / total_hours if total_hours else 0
    labor_pct = (total_labor / net_sales * 100) if net_sales else 0
    return {
        "splh": round(splh, 2),
        "labor_percentage": round(labor_pct, 1),
        "net_sales": round(net_sales, 2),
        "total_labor_hours": round(total_hours, 2),
        "total_labor_cost": round(total_labor, 2),
    }


# ── Schedule Fetching (ScheduledShift API) ──────────────────────────────────

def fetch_scheduled_shifts(date_str, location_id=None):
    """Fetch published scheduled shifts for a specific date."""
    loc_id = location_id or SQUARE_LOCATION_ID

    body = {
        "query": {
            "filter": {
                "location_ids": [loc_id],
                "scheduled_shift_statuses": ["PUBLISHED"],
                "assignment_status": "ASSIGNED",
                "workday": {
                    "match_shifts_by": "START_AT",
                    "date_range": {
                        "start_date": date_str,
                        "end_date": date_str,
                    },
                    "default_timezone": "America/Los_Angeles",
                },
            },
            "sort": {
                "field": "START_AT",
                "order": "ASC",
            },
        },
        "limit": 50,
    }

    url = f"{SQUARE_BASE_URL}/labor/scheduled-shifts/search"
    with httpx.Client(timeout=60) as client:
        resp = client.post(url, headers=square_headers(), json=body)
        if resp.status_code != 200:
            logger.error(f"Square Schedule API error: {resp.status_code} - {resp.text}")
            resp.raise_for_status()
        return resp.json()


def parse_scheduled_shifts(data, team_members=None):
    """Parse Square ScheduledShift objects into a clean format."""
    shifts = data.get("scheduled_shifts", [])
    parsed = []

    for shift in shifts:
        details = shift.get("published_shift_details") or shift.get("draft_shift_details", {})

        member_id = details.get("team_member_id", "")
        member_name = "Unassigned"
        if team_members and member_id in team_members:
            member_name = team_members[member_id]["name"]

        start_at = details.get("start_at", "")
        end_at = details.get("end_at", "")

        duration = 0
        if start_at and end_at:
            try:
                s = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
                e = datetime.fromisoformat(end_at.replace("Z", "+00:00"))
                duration = round((e - s).total_seconds() / 3600, 2)
            except (ValueError, TypeError):
                pass

        parsed.append({
            "id": shift.get("id", ""),
            "employee": member_name,
            "team_member_id": member_id,
            "position": details.get("job_title", "Team Member"),
            "start": start_at,
            "end": end_at,
            "duration": duration,
            "status": "published",
            "location": "Pixlcat SF - Ferry Building",
        })

    return parsed


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def home():
    """Serve dashboard or API info."""
    try:
        return send_file("dashboard.html")
    except FileNotFoundError:
        return jsonify({
            "service": "Square API for Pixlcat SF Ferry Building",
            "version": "2.0.0",
            "endpoints": [
                "GET /health",
                "GET /sales/yesterday",
                "GET /sales/<date>",
                "GET /sales/range?start=YYYY-MM-DD&end=YYYY-MM-DD",
                "GET /labor/yesterday",
                "GET /labor/<date>",
                "GET /schedule/today",
                "GET /schedule/tomorrow",
                "GET /schedule/<date>",
                "GET /catalog",
            ],
        })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "Square API for Pixlcat SF Ferry Building",
        "version": "2.0.0",
        "location_id": SQUARE_LOCATION_ID,
        "timezone": "America/Los_Angeles",
    })


@app.route("/sales/yesterday", methods=["GET"])
def sales_yesterday():
    """Get yesterday's sales."""
    try:
        date = get_business_date()
        start_at, end_at = date_to_rfc3339_range(date)
        catalog_objects = fetch_catalog()
        catalog_map, _ = build_catalog_map(catalog_objects)
        orders = fetch_all_orders(start_at, end_at)
        metrics = parse_sales_metrics(orders, catalog_map)
        timecards = fetch_timecards(start_at, end_at)
        team_members = fetch_team_members()
        labor = parse_labor_metrics(timecards, team_members)
        splh = calculate_splh(metrics, labor)
        return jsonify({
            "status": "success", "date": date.isoformat(),
            "day_of_week": date.strftime("%A"),
            "location": "Pixlcat SF - Ferry Building",
            "metrics": metrics, "labor": labor, "splh": splh,
        })
    except Exception as e:
        logger.error(f"Error fetching yesterday's sales: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/sales/<date_str>", methods=["GET"])
def sales_by_date(date_str):
    """Get sales for a specific date."""
    try:
        date = get_business_date(date_str)
        start_at, end_at = date_to_rfc3339_range(date)
        catalog_objects = fetch_catalog()
        catalog_map, _ = build_catalog_map(catalog_objects)
        orders = fetch_all_orders(start_at, end_at)
        metrics = parse_sales_metrics(orders, catalog_map)
        timecards = fetch_timecards(start_at, end_at)
        team_members = fetch_team_members()
        labor = parse_labor_metrics(timecards, team_members)
        splh = calculate_splh(metrics, labor)
        return jsonify({
            "status": "success", "date": date.isoformat(),
            "day_of_week": date.strftime("%A"),
            "location": "Pixlcat SF - Ferry Building",
            "metrics": metrics, "labor": labor, "splh": splh,
        })
    except ValueError:
        return jsonify({"status": "error", "error": "Invalid date format. Use YYYY-MM-DD"}), 400
    except Exception as e:
        logger.error(f"Error fetching sales for {date_str}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/sales/range", methods=["GET"])
def sales_range():
    """Get sales summary for a date range."""
    try:
        start_date = request.args.get("start")
        end_date = request.args.get("end")
        if not start_date or not end_date:
            return jsonify({"status": "error", "error": "Provide ?start=YYYY-MM-DD&end=YYYY-MM-DD"}), 400

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        start_at = datetime(start.year, start.month, start.day, tzinfo=TIMEZONE).isoformat()
        end_dt = datetime(end.year, end.month, end.day, tzinfo=TIMEZONE) + timedelta(days=1)
        end_at = end_dt.isoformat()

        catalog_objects = fetch_catalog()
        catalog_map, _ = build_catalog_map(catalog_objects)
        orders = fetch_all_orders(start_at, end_at)
        metrics = parse_sales_metrics(orders, catalog_map)
        timecards = fetch_timecards(start_at, end_at)
        team_members = fetch_team_members()
        labor = parse_labor_metrics(timecards, team_members)
        splh = calculate_splh(metrics, labor)

        days = (end - start).days + 1
        return jsonify({
            "status": "success", "start_date": start.isoformat(),
            "end_date": end.isoformat(), "days": days,
            "location": "Pixlcat SF - Ferry Building",
            "metrics": metrics, "labor": labor, "splh": splh,
            "daily_averages": {
                "avg_daily_revenue": round(metrics["net_sales"] / days, 2) if days else 0,
                "avg_daily_orders": round(metrics["total_orders"] / days, 1) if days else 0,
                "avg_daily_items": round(metrics["items_sold"] / days, 1) if days else 0,
            },
        })
    except ValueError:
        return jsonify({"status": "error", "error": "Invalid date format. Use YYYY-MM-DD"}), 400
    except Exception as e:
        logger.error(f"Error fetching sales range: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/labor/yesterday", methods=["GET"])
def labor_yesterday():
    """Get yesterday's labor data."""
    try:
        date = get_business_date()
        start_at, end_at = date_to_rfc3339_range(date)
        timecards = fetch_timecards(start_at, end_at)
        team_members = fetch_team_members()
        labor = parse_labor_metrics(timecards, team_members)
        return jsonify({
            "status": "success", "date": date.isoformat(),
            "day_of_week": date.strftime("%A"),
            "location": "Pixlcat SF - Ferry Building", "labor": labor,
        })
    except Exception as e:
        logger.error(f"Error fetching labor: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/labor/<date_str>", methods=["GET"])
def labor_by_date(date_str):
    """Get labor data for a specific date."""
    try:
        date = get_business_date(date_str)
        start_at, end_at = date_to_rfc3339_range(date)
        timecards = fetch_timecards(start_at, end_at)
        team_members = fetch_team_members()
        labor = parse_labor_metrics(timecards, team_members)
        return jsonify({
            "status": "success", "date": date.isoformat(),
            "day_of_week": date.strftime("%A"),
            "location": "Pixlcat SF - Ferry Building", "labor": labor,
        })
    except ValueError:
        return jsonify({"status": "error", "error": "Invalid date format. Use YYYY-MM-DD"}), 400
    except Exception as e:
        logger.error(f"Error fetching labor for {date_str}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


# ── Schedule Routes ─────────────────────────────────────────────────────────

@app.route("/schedule/today", methods=["GET"])
def schedule_today():
    """Get today's published scheduled shifts."""
    try:
        date = get_today_sf()
        date_str = date.isoformat()
        team_members = fetch_team_members()
        data = fetch_scheduled_shifts(date_str)
        shifts = parse_scheduled_shifts(data, team_members)
        return jsonify({
            "status": "success",
            "date": date.strftime("%A, %b %d, %Y"),
            "isoDate": date_str,
            "location": "Pixlcat SF - Ferry Building",
            "count": len(shifts),
            "shifts": shifts,
        })
    except httpx.HTTPStatusError as e:
        # If scheduling API not available (subscription required), fall back to labor
        logger.warning(f"Schedule API not available, falling back to labor: {e}")
        try:
            start_at, end_at = date_to_rfc3339_range(get_today_sf())
            timecards = fetch_timecards(start_at, end_at)
            team_members = fetch_team_members()
            labor = parse_labor_metrics(timecards, team_members)
            shifts = []
            for name, info in labor.get("team", {}).items():
                shifts.append({
                    "employee": name,
                    "position": info.get("job_title", "Team Member"),
                    "duration": info.get("hours", 0),
                    "location": "Pixlcat SF - Ferry Building",
                    "source": "timecard",
                })
            return jsonify({
                "status": "success",
                "date": get_today_sf().strftime("%A, %b %d, %Y"),
                "isoDate": get_today_sf().isoformat(),
                "location": "Pixlcat SF - Ferry Building",
                "count": len(shifts),
                "shifts": shifts,
                "note": "From timecards (scheduled shifts not available)",
            })
        except Exception as e2:
            return jsonify({"status": "error", "error": str(e2)}), 500
    except Exception as e:
        logger.error(f"Error fetching schedule: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/schedule/tomorrow", methods=["GET"])
def schedule_tomorrow():
    """Get tomorrow's published scheduled shifts."""
    try:
        date = get_tomorrow_sf()
        date_str = date.isoformat()
        team_members = fetch_team_members()
        data = fetch_scheduled_shifts(date_str)
        shifts = parse_scheduled_shifts(data, team_members)
        return jsonify({
            "status": "success",
            "date": date.strftime("%A, %b %d, %Y"),
            "isoDate": date_str,
            "location": "Pixlcat SF - Ferry Building",
            "count": len(shifts),
            "shifts": shifts,
        })
    except Exception as e:
        logger.error(f"Error fetching tomorrow schedule: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/schedule/<date_str>", methods=["GET"])
def schedule_by_date(date_str):
    """Get scheduled shifts for a specific date (YYYY-MM-DD)."""
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        team_members = fetch_team_members()
        data = fetch_scheduled_shifts(date_str)
        shifts = parse_scheduled_shifts(data, team_members)
        return jsonify({
            "status": "success",
            "date": date.strftime("%A, %b %d, %Y"),
            "isoDate": date_str,
            "location": "Pixlcat SF - Ferry Building",
            "count": len(shifts),
            "shifts": shifts,
        })
    except ValueError:
        return jsonify({"status": "error", "error": "Invalid date format. Use YYYY-MM-DD"}), 400
    except Exception as e:
        logger.error(f"Error fetching schedule for {date_str}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/catalog", methods=["GET"])
def get_catalog():
    """Return the full menu catalog."""
    try:
        catalog_objects = fetch_catalog()
        catalog_map, categories = build_catalog_map(catalog_objects)
        by_category = {}
        for var_id, var_info in catalog_map.items():
            cat = var_info["category"]
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append({
                "id": var_id,
                "name": var_info["name"],
                "item_name": var_info["item_name"],
            })
        return jsonify({
            "status": "success",
            "location": "Pixlcat SF - Ferry Building",
            "categories": by_category,
            "total_items": len(catalog_map),
        })
    except Exception as e:
        logger.error(f"Error fetching catalog: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500



# ── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

