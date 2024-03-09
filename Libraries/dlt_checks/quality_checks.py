# Define rules for data quality checks

# rules_buyers
rules_buyers = {
    "valid_id":"id IS NOT NULL",
    "valid_name": "name IS NOT NULL",
    "valid_username": "username IS NOT NULL",
    "valid_email":"email IS NOT NULL AND email LIKE '%@%'"
}
quarantine_rules_buyers = "NOT({0})".format(" AND ".join(rules_buyers.values()))


# rules_bids
rules_bids = {
    "valid_bid":"bid IS NOT NULL",
    "valid_bidder":"bidder IS NOT NULL"
}
quarantine_rules_bids = "NOT({0})".format(" AND ".join(rules_bids.values()))

# rules_auctions
rules_auctions = {
    "valid_auctionid":"auctionid IS NOT NULL", 
    "valid_auction_type":"auction_type IS NOT NULL"
}