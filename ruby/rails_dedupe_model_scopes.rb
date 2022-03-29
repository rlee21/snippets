scope :latest_by_exited_at, lambda {
                              joins('LEFT JOIN inventories AS inventories2
                                            ON inventories.vin = inventories2.vin
                                           AND inventories.exited_at < inventories2.exited_at')
                                .where(inventories2: { vin: nil })
                            }
# SELECT inv1.id, inv1.vin, inv1.exited_at
# FROM inventories AS inv1
# LEFT JOIN inventories AS inv2
#        ON inv1.vin = inv2.vin AND inv1.exited_at < inv2.exited_at
# WHERE inv2.vin IS NULL
#   AND inv1.vin = 'AB1CD7EF9GH677954';

#################################################################################################
scope :latest_by_exited_at, -> { where(exited_at: group(:vin).maximum(:exited_at).values.first) }
# SELECT id, vin, exited_at
# FROM inventories
# WHERE vin = 'AB1CD7EF9GH677954'
#   AND exited_at = (SELECT MAX(exited_at) FROM inventories WHERE vin = 'AB1CD7EF9GH677954');

#################################################################################################
scope :latest_by_exited_at, lambda {
                              joins('INNER JOIN (SELECT vin, MAX(exited_at) AS max_exited_at
                                                 FROM inventories
                                                 GROUP BY vin) base
                                            ON inventories.vin = base.vin
                                           AND inventories.exited_at = base.max_exited_at')
                            }
# SELECT inv.id, inv.vin, inv.exited_at
# FROM inventories AS inv
# INNER JOIN (SELECT vin, MAX(exited_at) AS max_exited_at
#             FROM inventories
#             WHERE vin = 'AB1CD7EF9GH677954'
#             GROUP BY vin) AS max_dt
#         ON inv.vin = max_dt.vin AND inv.exited_at = max_dt.max_exited_at;

#################################################################################################
# SELECT id, vin, exited_at
# FROM (SELECT id, vin, exited_at,
#        ROW_NUMBER() OVER (PARTITION BY vin ORDER BY exited_at DESC) AS exited_at_sort
#       FROM inventories
#       WHERE vin = 'AB1CD7EF9GH677954') AS base
# WHERE base.exited_at_sort = 1;
