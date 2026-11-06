-- Jeu de données minimal pour test/performance.test.js (base de TEST uniquement : tables vidées).
TRUNCATE mvtart, articles, artfou1, fouident CASCADE;

INSERT INTO fouident (code, nom) VALUES ('L027', 'LIDIS'), ('S028', 'SUPERGROUP'), ('L0270', 'AUTRE');

INSERT INTO articles (no_id, codein, libelle1) VALUES (1, 'A1', 'COCA 50CL'), (2, 'A2', 'CHIPS');

INSERT INTO artfou1 (no_id, art_no_id, code, preference) VALUES (11, 1, 'L027 ', 1), (20, 2, 'S028', 1);

INSERT INTO mvtart (no_id, artnoid, datmvt, site, genremvt, mvttypnoid, qtemvt, mntmvtht, mntmvtttc, margemvt) VALUES
  (100, 1, '2026-01-05',          '292', 1, NULL, 10,  50,  60,  0),   -- réception : pas du CA
  (110, 1, '2026-02-01',          '292', 3, 11,   -2, -10, -12,  4),
  (111, 1, '2026-03-10',          '292', 3, 11,   -1,  -5,  -6,  2),
  (112, 1, '2026-03-11',          '292', 3, 12,    1,   5,   6, -2),   -- retour client (type 12)
  (113, 1, '2026-03-31 18:30',    '292', 3, NULL, -1,  -5,  -6,  2),   -- ligne sans type, dernier jour en soirée
  (114, 1, '2026-03-12',          '292', 9, NULL, -3, -15, -18,  0),   -- démarque : pas du CA
  (115, 1, '2026-03-20',          '292', 3, NULL,  1,   5,   6, -2),   -- retour sans type (montant positif)
  (120, 2, '2026-02-15',          '292', 3, 11,   -4, -20, -24, 10),
  (130, 2, '2026-02-15',          '579', 3, 11,   -1,  -5,  -6,  2),   -- autre magasin
  (140, 2, '2026-04-01',          '292', 3, 11,   -1,  -5,  -6,  2);   -- hors période

-- Magasin 700 : A3 vendu puis rapporté le 15/02 ; A4 seulement rapporté le 15/02 ; A3 seulement rapporté en mars
INSERT INTO articles (no_id, codein, libelle1) VALUES (3, 'A3', 'VENDU PUIS RAPPORTE'), (4, 'A4', 'RETOUR SEUL');
INSERT INTO mvtart (no_id, artnoid, datmvt, site, genremvt, mvttypnoid, qtemvt, mntmvtht, mntmvtttc, margemvt) VALUES
  (150, 3, '2026-02-15 10:00', '700', 3, 11, -2, -10, -12,  4),
  (151, 3, '2026-02-15 16:00', '700', 3, 12,  1,   5,   6, -2),
  (152, 4, '2026-02-15 11:00', '700', 3, 12,  1,   3,   4, -1),
  (153, 3, '2026-03-05 10:00', '700', 3, 12,  1,   5,   6, -2);
