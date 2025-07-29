WITH ranked_itm_rfd06 AS (
    SELECT 
        idtbl_gst_rfd_data,
        idtbl_gst_rfd_data_itm,
        ROW_NUMBER() OVER (PARTITION BY idtbl_gst_rfd_data ORDER BY idtbl_gst_rfd_data_itm DESC) AS rn
    FROM 
        public.tbl_gst_rfd_data_itm
    WHERE 
        itemname = 'RFD06'
),
ranked_itm_appl AS (
    SELECT 
        idtbl_gst_rfd_data,
        idtbl_gst_rfd_data_itm,
        ROW_NUMBER() OVER (PARTITION BY idtbl_gst_rfd_data ORDER BY idtbl_gst_rfd_data_itm DESC) AS rn
    FROM 
        public.tbl_gst_rfd_data_itm
    WHERE 
        itemname = 'APPL'
),
max_id AS (
    SELECT 
        crn, 
        MAX(idtbl_gst_rfd_data) AS max_idtbl_gst_rfd_data
    FROM 
        public.tbl_gst_rfd_data
    WHERE 
        casety_cd = 'RFUND'
    GROUP BY 
        crn
)
SELECT 
    m.h1 as wards,
    'Zone ' || m.h2 as zones,
    m.trdnm as tradeName,
    COALESCE(m.rgfmdt,'') as registrationDate,
    COALESCE(m.canc_dt,'') as cancelledDate,
    m.authstatus as registrationStatus,
    m.appr_auth as authority,
    a.gstin as gstin, 
    a.crn AS arn, 
    coalesce(a.dof,'') AS applicationdate,
    a.status_cd AS statusCode,
    coalesce(l.dt,'') AS sanctionedDate,
    coalesce(z.dg, '') AS designation,
    coalesce(z.nm, '') AS officerName,
    coalesce(k.remarks, '') AS sanctionedRemarks,
    n.rfdrsn,
    n.frmtxprd,
    n.totxprd,
    n.totrfdamt,
    n.statementtype,
    a.idtbl_gst_rfd_data,
    b.idtbl_gst_rfd_data_itm AS rfd06_idtbl_gst_rfd_data_itm,
    c.idtbl_gst_rfd_data_itm AS appl_idtbl_gst_rfd_data_itm,	
    
    (coalesce(o.igrfclm_tax,0)+coalesce(o.igrfclm_intr,0)+coalesce(o.igrfclm_fee,0)+coalesce(o.igrfclm_pen,0)+coalesce(o.igrfclm_oth,0)) as igst_claim,
    (coalesce(o.cgrfclm_tax,0)+coalesce(o.cgrfclm_intr,0)+coalesce(o.cgrfclm_fee,0)+coalesce(o.cgrfclm_pen,0)+coalesce(o.cgrfclm_oth,0)) as cgst_claim,
    (coalesce(o.sgrfclm_tax,0)+coalesce(o.sgrfclm_intr,0)+coalesce(o.sgrfclm_fee,0)+coalesce(o.sgrfclm_pen,0)+coalesce(o.sgrfclm_oth,0)) as sgst_claim,
	(coalesce(o.csrdfclm_tax,0)+coalesce(o.csrdfclm_intr,0)+coalesce(o.csrdfclm_fee,0)+coalesce(o.csrdfclm_pen,0)+coalesce(o.csrdfclm_oth,0)) as cess_claim,
    
    COALESCE(k.sancamtdtl_igst_amtdtl_inadmamt_tax, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_inadmamt_intr, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_inadmamt_pen, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_inadmamt_fee, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_inadmamt_oth, 0) AS igst_inadmamt,
    COALESCE(k.sancamtdtl_cgst_amtdtl_inadmamt_tax, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_inadmamt_intr, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_inadmamt_pen, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_inadmamt_fee, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_inadmamt_oth, 0) AS cgst_inadmamt,
    COALESCE(k.sancamtdtl_sgst_amtdtl_inadmamt_tax, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_inadmamt_intr, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_inadmamt_pen, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_inadmamt_fee, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_inadmamt_oth, 0) AS sgst_inadmamt,
    COALESCE(k.sancamtdtl_cess_amtdtl_inadmamt_tax, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_inadmamt_intr, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_inadmamt_pen, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_inadmamt_fee, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_inadmamt_oth, 0) AS cess_inadmamt,
    
    COALESCE(k.sancamtdtl_igst_amtdtl_netamt_tax, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_netamt_intr, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_netamt_pen, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_netamt_fee, 0) + COALESCE(k.sancamtdtl_igst_amtdtl_netamt_oth, 0) AS igst_netamt,
    COALESCE(k.sancamtdtl_cgst_amtdtl_netamt_tax, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_netamt_intr, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_netamt_pen, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_netamt_fee, 0) + COALESCE(k.sancamtdtl_cgst_amtdtl_netamt_oth, 0) AS cgst_netamt,
    COALESCE(k.sancamtdtl_sgst_amtdtl_netamt_tax, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_netamt_intr, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_netamt_pen, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_netamt_fee, 0) + COALESCE(k.sancamtdtl_sgst_amtdtl_netamt_oth, 0) AS sgst_netamt,
    COALESCE(k.sancamtdtl_cess_amtdtl_netamt_tax, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_netamt_intr, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_netamt_pen, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_netamt_fee, 0) + COALESCE(k.sancamtdtl_cess_amtdtl_netamt_oth, 0) AS cess_netamt
FROM 
    public.tbl_gst_rfd_data a
JOIN 
    max_id m_id ON a.idtbl_gst_rfd_data = m_id.max_idtbl_gst_rfd_data
LEFT JOIN 
    ranked_itm_rfd06 b ON a.idtbl_gst_rfd_data = b.idtbl_gst_rfd_data AND b.rn = 1
LEFT JOIN 
    ranked_itm_appl c ON a.idtbl_gst_rfd_data = c.idtbl_gst_rfd_data AND c.rn = 1
LEFT JOIN 
    public.tbl_gst_rfd_data_itm_san_ord_data i ON CASE 
        WHEN b.idtbl_gst_rfd_data_itm IS NOT NULL THEN b.idtbl_gst_rfd_data_itm
        ELSE c.idtbl_gst_rfd_data_itm
    END = i.idtbl_gst_rfd_data_itm
LEFT JOIN 
    public.tbl_gst_rfd_data_itm_san_ord_data_stdl j ON i.idtbl_gst_rfd_data_itm_san_ord_data = j.idtbl_gst_rfd_data_itm_san_ord_data
LEFT JOIN 
    public.tbl_gst_rfd_data_itm_san_ord_data_stdl_vo k ON j.idtbl_gst_rfd_data_itm_san_ord_data_stdl = k.idtbl_gst_rfd_data_itm_san_ord_data_stdl
LEFT JOIN 
    public.tbl_gst_rfd_data_itm_san_ord_data_todtls l ON i.idtbl_gst_rfd_data_itm_san_ord_data = l.idtbl_gst_rfd_data_itm_san_ord_data
LEFT JOIN
    public.tbl_gst_rfd_data_itm_rfdapp n on c.idtbl_gst_rfd_data_itm = n.idtbl_gst_rfd_data_itm
LEFT JOIN 
    public.tbl_gst_rfd_data_itm_rfdapp_refclaim o on n.idtbl_gst_rfd_data_itm_rfdapp = o.idtbl_gst_rfd_data_itm_rfdapp
LEFT JOIN
    public.t_all_delers_api_v_t_upd m on a.gstin = m.gstin
LEFT JOIN 
	public.tbl_gst_rfd_data_itm_rfdnotice x on CASE 
        WHEN b.idtbl_gst_rfd_data_itm IS NOT NULL THEN b.idtbl_gst_rfd_data_itm
        ELSE c.idtbl_gst_rfd_data_itm
    END = x.idtbl_gst_rfd_data_itm
LEFT JOIN 
	public.tbl_gst_rfd_data_itm_rfdnotice_data y on y.idtbl_gst_rfd_data_itm_rfdnotice = x.idtbl_gst_rfd_data_itm_rfdnotice
LEFT JOIN 
	public.tbl_gst_rfd_data_itm_rfdnotice_data_todtls z on z.idtbl_gst_rfd_data_itm_rfdnotice_data = y.idtbl_gst_rfd_data_itm_rfdnotice_data
WHERE 
    a.casety_cd = 'RFUND'