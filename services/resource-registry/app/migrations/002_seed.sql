-- Demo seed data: 5 regions x (vehicles, supply kits, response teams)
INSERT INTO resources (kind, subtype, name, region, capacity, latitude, longitude)
SELECT k.kind, k.subtype, k.subtype || '-' || r.region || '-' || g.n, r.region, k.capacity, r.lat, r.lon
FROM (VALUES ('north', 28.61, 77.21), ('south', 13.08, 80.27), ('east', 22.57, 88.36),
             ('west', 19.07, 72.88), ('central', 23.26, 77.41)) AS r(region, lat, lon)
CROSS JOIN (VALUES ('VEHICLE', 'ambulance', 4), ('VEHICLE', 'fire-truck', 6), ('VEHICLE', 'rescue-boat', 8),
                   ('SUPPLY', 'water-kit', 100), ('SUPPLY', 'medical-kit', 50),
                   ('TEAM', 'medical-team', 5), ('TEAM', 'search-rescue-team', 8)) AS k(kind, subtype, capacity)
CROSS JOIN generate_series(1, 3) AS g(n);
