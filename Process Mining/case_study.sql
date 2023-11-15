ATTACH DATABASE 'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining\EKPO.db' AS EKPO;
ATTACH DATABASE 'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining\EKKO.db' AS EKKO;
ATTACH DATABASE 'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining\CDHDR.db' AS CDHDR;
ATTACH DATABASE 'C:\Users\vladimir.shpilkin\Desktop\Learning\Python-Projects\process_mining\CDPOS.db' AS CDPOS;

SELECT
    EKPO.MANDT AS client,
    EKPO.EBELN AS po_number,
    EKPO.EBELP AS item_number,
    SUM(DISTINCT EKPO.NETWR) AS total_unique_net_value,
    date(SUBSTR(CDHDR.UDATE, 1, 10)) AS update_date
FROM
    EKPO
LEFT JOIN
    EKKO ON EKPO.MANDT = EKKO.MANDT AND EKPO.EBELN = EKKO.EBELN
LEFT JOIN
    CDHDR ON EKPO.MANDT = CDHDR.MANDANT AND EKPO.EBELN = CDHDR.OBJECTID
LEFT JOIN
    CDPOS ON EKPO.MANDT = CDPOS.MANDANT AND EKPO.EBELN = CDPOS.OBJECTID
GROUP BY
    po_number,update_date
ORDER BY
    po_number,update_date;
