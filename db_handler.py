from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    cur.execute("SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item")
    new_sk = cur.fetchone()[0]
    start_date = f"{new_item.start_year}-01-01"

    query = """
        INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, 
                          i_brand, i_category, i_manufact, i_current_price, i_num_owned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cur.execute(query, (
        new_sk,
        new_item.item_id,
        start_date,
        new_item.product_name,
        new_item.brand,
        new_item.category,
        new_item.manufact,
        new_item.current_price,
        new_item.num_owned
    ))


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    street_part, city_part, state_zip_part = new_customer.address.split(", ")
    street_tokens = street_part.split(" ", 1)
    street_number = street_tokens[0]
    street_name = street_tokens[1] if len(street_tokens) > 1 else ""
    city = city_part
    state_zip_tokens = state_zip_part.split(" ", 1)
    state = state_zip_tokens[0]
    zip_code = state_zip_tokens[1] if len(state_zip_tokens) > 1 else ""
 
    cur.execute("SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address")
    new_addr_sk = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip) "
        "VALUES (?, ?, ?, ?, ?, ?)", (new_addr_sk, street_number, street_name, city, state, zip_code)
        )
 
    name_tokens = new_customer.name.split(" ", 1)
    first_name = name_tokens[0]
    last_name = name_tokens[1] if len(name_tokens) > 1 else ""
 
    cur.execute("SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer")
    new_cust_sk = cur.fetchone()[0]
 
    cur.execute(
        "INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk) "
        "VALUES (?, ?, ?, ?, ?, ?)", (new_cust_sk, new_customer.customer_id, first_name, last_name, new_customer.email, new_addr_sk)
    )


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    cur.execute(
        "SELECT c_customer_sk, c_current_addr_sk FROM customer WHERE c_customer_id = ?",
        (original_customer_id,)
    )
    row = cur.fetchone()
    if row is None:
        return
    cust_sk, addr_sk = row
 
    if new_customer.address is not None and addr_sk is not None:
        street_part, city_part, state_zip_part = new_customer.address.split(", ")
        street_tokens = street_part.split(" ", 1)
        street_number = street_tokens[0]
        street_name = street_tokens[1] if len(street_tokens) > 1 else ""
        city = city_part
        state_zip_tokens = state_zip_part.split(" ", 1)
        state = state_zip_tokens[0]
        zip_code = state_zip_tokens[1] if len(state_zip_tokens) > 1 else ""
 
        cur.execute(
            "UPDATE customer_address SET ca_street_number = ?, ca_street_name = ?, "
            "ca_city = ?, ca_state = ?, ca_zip = ? WHERE ca_address_sk = ?",
            (street_number, street_name, city, state, zip_code, addr_sk)
        )
 
    set_clauses = []
    params = []
 
    if new_customer.customer_id is not None:
        set_clauses.append("c_customer_id = ?")
        params.append(new_customer.customer_id)
 
    if new_customer.name is not None:
        name_tokens = new_customer.name.split(" ", 1)
        first_name = name_tokens[0]
        last_name = name_tokens[1] if len(name_tokens) > 1 else ""
        set_clauses.append("c_first_name = ?")
        params.append(first_name)
        set_clauses.append("c_last_name = ?")
        params.append(last_name)
 
    if new_customer.email is not None:
        set_clauses.append("c_email_address = ?")
        params.append(new_customer.email)
 
    if set_clauses:
        params.append(cust_sk)
        query = f"UPDATE customer SET {', '.join(set_clauses)} WHERE c_customer_sk = ?"
        cur.execute(query, params)


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    today = date.today()
    due = today + timedelta(days=14)
    
    query = """
        INSERT INTO rental (item_id, customer_id, rental_date, due_date)
        VALUES (?, ?, ?, ?)
    """
    cur.execute(query, (item_id, customer_id, today.isoformat(), due.isoformat()))


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    new_place = line_length(item_id) + 1
    cur.execute(
        "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
        (item_id, customer_id, new_place)
    )
    return new_place

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    cur.execute(
        "DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1",
        (item_id,)
    )
 
    cur.execute(
        "UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?",
        (item_id,)
    )


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    cur.execute(
        "UPDATE rental SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)"
        "WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    if not item_id:
        return -1
        
    cur.execute("SELECT i_num_owned FROM item WHERE i_item_id = ?", (item_id,))
    row = cur.fetchone()
    
    if row is None:
        return -1
        
    total_owned = row[0]
    
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id = ?", (item_id,))
    active_rentals = cur.fetchone()[0]
    
    return total_owned - active_rentals


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    cur.execute(
        "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )
    row = cur.fetchone()
    return int(row[0]) if row is not None else -1


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    cur.execute(
        "SELECT COUNT(*) FROM waitlist WHERE item_id = ?",
        (item_id,)
    )
    return cur.fetchone()[0]


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

