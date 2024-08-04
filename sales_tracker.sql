-- sales_tracker.sql
WITH 
KW_sales AS (
  SELECT 
    block_timestamp, 
    tx_id, 
    event_attributes:"1_contract_address"::string AS nft_contract, 
    event_attributes:token_id::string AS token_id, 
    event_attributes:auction_id::string AS auction_id, 
    event_attributes:recipient::string AS new_owner, 
    'KnoWhere.art' AS market 
  FROM 
    terra.msg_events 
  WHERE 
    event_type = 'from_contract' 
    AND event_attributes:"0_action"::string = 'settle' 
    AND event_attributes:"0_contract_address"::string = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j' -- KW contract
    AND nft_contract = 'terra1jp5fjj7rlc0erw4z3qr5zuvmg2w49pfzyhvsnk' --monkeez contract
),

KW_get_saleprice AS (
  SELECT 
    msg_value:sender::string AS new_owner, 
    msg_value:execute_msg:place_bid:auction_id::string AS auction_id, 
    msg_value:coins[0]:amount::integer / 1000000 AS lunaX
  FROM 
    terra.msgs 
  WHERE 
    msg_value:contract::string = 'terra12v8vrgntasf37xpj282szqpdyad7dgmkgnq60j' -- KW contract
    AND msg_value:execute_msg:place_bid:auction_id IS NOT NULL 
    AND tx_status = 'SUCCEEDED'
),

KW_sale_with_price AS (
  SELECT 
    block_timestamp, 
    tx_id, 
    nft_contract, 
    token_id,
    lunaX, 
    new_owner,
    market  
  FROM 
    KW_sales 
    LEFT JOIN KW_get_saleprice USING (new_owner, auction_id)
),

Kw_get_correct_tx AS (
  SELECT 
    t.block_timestamp, 
    t.tx_id, 
    t.nft_contract, 
    t.token_id,
    t.lunaX, 
    t.new_owner,
    t.market  
  FROM 
    KW_sale_with_price t
  INNER JOIN (
    SELECT  
      block_timestamp, 
      tx_id, 
      nft_contract, 
      token_id,
      MAX(lunaX) AS luna, 
      new_owner,
      market 
    FROM 
      KW_sale_with_price
    GROUP BY  
      block_timestamp, 
      tx_id, 
      nft_contract, 
      token_id, 
      new_owner,
      market
  ) tmp ON 
    t.block_timestamp = tmp.block_timestamp 
    AND t.tx_id = tmp.tx_id 
    AND t.nft_contract = tmp.nft_contract 
    AND t.token_id = tmp.token_id 
    AND t.lunaX = tmp.luna 
    AND t.new_owner = tmp.new_owner 
    AND t.market = tmp.market 
),

RE_sales AS (
  SELECT 
    block_timestamp, 
    tx_id, 
    msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:contract_addr::string AS nft_contract, 
    msg_value:execute_msg:ledger_proxy:msg:execute_order:order:order:maker_asset:info:nft:token_id::string AS tokenid,
    msg_value:coins[0]:amount::integer / 1000000 AS luna,
    msg_value:sender::string AS owner, 
    'RandomEarth.io' AS market 
  FROM 
    terra.msgs 
  WHERE 
    msg_value:contract::string = 'terra1eek0ymmhyzja60830xhzm7k7jkrk99a60q2z2t' -- RE Sale Contract
    AND nft_contract = 'terra1jp5fjj7rlc0erw4z3qr5zuvmg2w49pfzyhvsnk' --monkeez contract
    AND tx_status = 'SUCCEEDED'
),

All_sales AS (
  -- Combining RE and KW Sales
  SELECT 
    * 
  FROM 
    RE_sales 
  UNION ALL 
  SELECT 
    * 
  FROM 
    Kw_get_correct_tx
)

SELECT 
  -- Main query
  *
FROM 
  All_sales
ORDER BY 
  block_timestamp DESC;
