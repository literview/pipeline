"""
Major journals by discipline for LiterView.
Scope (active): political-science, public-policy, sociology, psychology, economics, business (6 disciplines).
Removed 2026-04-15 to keep DB within Neon Free tier: communications, education, law.
Full 9-discipline config preserved in journals_config_full.py for future re-expansion.
"""

# OpenAlex Source ID -> Journal Name
# ISSN stored in JOURNAL_ISSNS for Crossref fallback

JOURNALS_BY_DISCIPLINE = {
    # =========================================================================
    # SOCIAL SCIENCES
    # =========================================================================

    "political-science": {
        "S90314269": "American Journal of Political Science",
        "S176007004": "American Political Science Review",
        "S72844074": "American Politics Research",
        "S8194976": "Annual Review of Political Science",
        "S95691132": "British Journal of Political Science",
        "S105556297": "Comparative Political Studies",
        "S2764871768": "Comparative Politics",
        "S110268115": "Democratization",
        "S124584455": "Electoral Studies",
        "S40975480": "European Journal of International Relations",
        "S135158421": "European Journal of Political Research",
        "S62375027": "Governance",
        "S29411750": "Government and Opposition",
        "S160686149": "International Organization",
        "S99767407": "International Security",
        "S91639875": "International Studies Quarterly",
        "S20177303": "Journal of Conflict Resolution",
        "S151119180": "Journal of Democracy",
        "S22371039": "Journal of European Public Policy",
        "S71119591": "Journal of Peace Research",
        "S25370267": "Journal of Policy Analysis and Management",
        "S81746323": "Journal of Political Philosophy",
        "S95650557": "Journal of Politics",
        "S48869744": "Legislative Studies Quarterly",
        "S975761300": "PS: Political Science & Politics",
        "S6959732": "Party Politics",
        "S119514724": "Policy Studies Journal",
        "S29331042": "Political Analysis",
        "S110036823": "Political Behavior",
        "S108349823": "Political Research Quarterly",
        "S161027966": "Political Science Quarterly",
        "S2764571748": "Political Science Research and Methods",
        "S25369639": "Political Theory",
        "S103858575": "Presidential Studies Quarterly",
        "S76877748": "Public Administration Review",
        "S135539873": "Public Opinion Quarterly",
        "S44648735": "Quarterly Journal of Political Science",
        "S108218269": "Regulation & Governance",
        "S2738497388": "Research & Politics",
        "S131264395": "Review of International Studies",
        "S200906791": "Security Studies",
        "S148921865": "State Politics & Policy Quarterly",
        "S179028992": "West European Politics",
        "S143110675": "World Politics",
    },

    "public-policy": {
        "S5465880": "Administration & Society",
        "S106702950": "American Review of Public Administration",  # ISSN: 0275-0740
        "S62375027": "Governance",
        "S63571029": "International Public Management Journal",
        "S16140064": "International Review of Administrative Sciences",
        "S77082647": "Journal of Comparative Policy Analysis",  # ISSN: 1387-6988
        "S22371039": "Journal of European Public Policy",
        "S25370267": "Journal of Policy Analysis and Management",
        "S169433491": "Journal of Public Administration Research and Theory",
        "S76906069": "Journal of Public Policy",
        "S17167983": "Policy & Politics",
        "S197895017": "Policy Sciences",
        "S119514724": "Policy Studies Journal",
        "S201221823": "Public Administration",
        "S76877748": "Public Administration Review",
        "S37623806": "Public Management Review",
        "S17185278": "Public Policy and Administration",
        "S108218269": "Regulation & Governance",
        "S54913114": "Review of Policy Research",
    },

    "sociology": {
        "S143668711": "Administrative Science Quarterly",
        "S20589029": "American Behavioral Scientist",
        "S122471516": "American Journal of Sociology",
        "S157620343": "American Sociological Review",
        "S61274580": "Annual Review of Sociology",
        "S173252385": "British Journal of Sociology",
        "S161221444": "City and Community",
        "S67613863": "Criminology",
        "S30543418": "Demography",
        "S96629499": "Ethnic and Racial Studies",
        "S159327246": "European Sociological Review",
        "S113116626": "Gender & Society",
        "S90857023": "International Migration Review",  # ISSN: 0197-9183
        "S17740374": "Journal of Contemporary Ethnography",
        "S149872823": "Journal of Ethnic and Migration Studies",  # ISSN: 1369-183X
        "S122290859": "Journal of Health and Social Behavior",
        "S146344": "Journal of Marriage and Family",
        "S173027916": "Journal of Research in Crime and Delinquency",
        "S44706263": "Law & Society Review",
        "S190942573": "Mobilization",
        "S98355519": "Poetics",  # ISSN: 0304-422X
        "S202512192": "Population Studies",  # ISSN: 0032-4728
        "S32314625": "Population and Development Review",
        "S143967217": "Qualitative Sociology",
        "S159612214": "Research in Social Stratification and Mobility",
        "S130611943": "Social Forces",
        "S135892541": "Social Movement Studies",
        "S26186134": "Social Networks",  # ISSN: 0378-8733
        "S129389861": "Social Problems",
        "S131591925": "Social Psychology Quarterly",
        "S59311786": "Social Science Research",
        "S193359815": "Sociological Forum",  # ISSN: 0884-8971
        "S9536269": "Sociological Methods & Research",
        "S195570583": "Sociological Perspectives",  # ISSN: 0731-1214
        "S66666449": "Sociological Quarterly",  # ISSN: 0038-0253
        "S60621485": "Sociological Theory",
        "S36718530": "Sociology of Education",
        "S136778369": "Sociology of Religion",  # ISSN: 1069-4404
        "S4210195209": "Socius",
        "S118419795": "Theory and Society",
        "S50366009": "Urban Studies",
        "S51211322": "Work and Occupations",
    },

    # =========================================================================
    # PSYCHOLOGY
    # =========================================================================

    "psychology": {
        "S90670110": "Annual Review of Psychology",
        "S137478622": "Behavior Research Methods",
        "S109723506": "Child Development",
        "S2764897075": "Clinical Psychological Science",
        "S88198767": "Cognition",
        "S36783443": "Cognitive Psychology",  # ISSN: 0010-0285
        "S78735424": "Cognitive Science",
        "S116196263": "Cortex",  # ISSN: 0010-9452
        "S126635229": "Developmental Psychology",
        "S154906575": "Developmental Science",  # ISSN: 1363-755X
        "S118198609": "Emotion",
        "S10288104": "European Journal of Social Psychology",
        "S34137071": "Health Psychology",
        "S67039580": "Infant Behavior and Development",  # ISSN: 0163-6383
        "S121947241": "Journal of Abnormal Psychology",
        "S166002381": "Journal of Applied Psychology",
        "S123144817": "Journal of Cognitive Neuroscience",
        "S118719888": "Journal of Consulting and Clinical Psychology",
        "S201005279": "Journal of Experimental Child Psychology",  # ISSN: 0022-0965
        "S62013203": "Journal of Experimental Psychology: General",
        "S18666220": "Journal of Experimental Psychology: Learning, Memory, and Cognition",
        "S12410666": "Journal of Experimental Social Psychology",
        "S50147421": "Journal of Family Psychology",
        "S53787413": "Journal of Memory and Language",
        "S160573970": "Journal of Organizational Behavior",
        "S29984966": "Journal of Personality and Social Psychology",
        "S156995526": "Journal of Research in Personality",
        "S25746158": "Journal of Vocational Behavior",
        "S114265899": "Memory & Cognition",  # ISSN: 0090-502X
        "S64250036": "Multivariate Behavioral Research",
        "S2764866340": "Nature Human Behaviour",
        "S165368631": "Neuropsychologia",  # ISSN: 0028-3932
        "S97426521": "Neuropsychology",
        "S64744539": "Organizational Behavior and Human Decision Processes",
        "S187348256": "Personality and Social Psychology Bulletin",
        "S84664706": "Personnel Psychology",
        "S27228949": "Perspectives on Psychological Science",
        "S75627607": "Psychological Bulletin",
        "S71144982": "Psychological Medicine",
        "S45419345": "Psychological Methods",
        "S35223124": "Psychological Review",
        "S58854535": "Psychological Science",
        "S55618883": "Psychology and Aging",  # ISSN: 0882-7974
        "S138679565": "Psychonomic Bulletin & Review",  # ISSN: 1069-9384
        "S167723465": "Psychophysiology",
        "S177779441": "Social Cognition",
        "S192051125": "Trends in Cognitive Sciences",
    },

    # =========================================================================
    # ECONOMICS
    # =========================================================================

    "economics": {
        "S42893225": "American Economic Journal: Applied Economics",
        "S158011328": "American Economic Journal: Economic Policy",
        "S170166683": "American Economic Journal: Macroeconomics",
        "S96919139": "American Economic Journal: Microeconomics",
        "S23254222": "American Economic Review",
        "S4210174288": "American Economic Review: Insights",
        "S4210173904": "Brookings Papers on Economic Activity",  # ISSN: 0007-2303
        "S95464858": "Econometrica",
        "S45992627": "Economic Journal",
        "S99606308": "Economic Policy",  # ISSN: 0266-4658
        "S69338747": "European Economic Review",
        "S181493553": "Experimental Economics",
        "S94044085": "Games and Economic Behavior",
        "S21260181": "IMF Economic Review",  # ISSN: 2041-4161
        "S179979277": "International Economic Review",  # ISSN: 0020-6598
        "S85739584": "Journal of Applied Econometrics",  # ISSN: 0883-7252
        "S2876017": "Journal of Banking & Finance",
        "S152282257": "Journal of Corporate Finance",
        "S101209419": "Journal of Development Economics",
        "S127742747": "Journal of Econometrics",
        "S62201805": "Journal of Economic Behavior & Organization",
        "S44585919": "Journal of Economic Dynamics and Control",  # ISSN: 0165-1889
        "S181171746": "Journal of Economic Growth",
        "S127708089": "Journal of Economic Literature",
        "S72880728": "Journal of Economic Perspectives",
        "S149131268": "Journal of Economic Theory",
        "S200872515": "Journal of Environmental Economics and Management",
        "S5353659": "Journal of Finance",
        "S149240962": "Journal of Financial Economics",
        "S5984737": "Journal of Financial Intermediation",
        "S166621295": "Journal of Health Economics",
        "S198098467": "Journal of International Economics",
        "S8557221": "Journal of Labor Economics",
        "S176795851": "Journal of Macroeconomics",
        "S6711363": "Journal of Monetary Economics",
        "S95323914": "Journal of Political Economy",
        "S199447588": "Journal of Public Economics",
        "S147692640": "Journal of Urban Economics",
        "S165087003": "Journal of the European Economic Association",
        "S136745531": "Macroeconomic Dynamics",
        "S92565720": "Oxford Bulletin of Economics and Statistics",  # ISSN: 0305-9049
        "S203860005": "Quarterly Journal of Economics",
        "S34139249": "RAND Journal of Economics",
        "S163499366": "Review of Economic Dynamics",  # ISSN: 1094-2025
        "S88935262": "Review of Economic Studies",
        "S180061323": "Review of Economics and Statistics",
        "S170137484": "Review of Financial Studies",
        "S78091837": "Review of World Economics",
        "S62957338": "The Journal of Human Resources",
        "S998097505": "The Journal of Law and Economics",
        "S2735890421": "World Bank Economic Review",  # ISSN: 0258-6770
    },

    # =========================================================================
    # BUSINESS / MANAGEMENT
    # =========================================================================

    "business": {
        "S27614628": "Academy of Management Annals",  # ISSN: 1941-6067
        "S117778295": "Academy of Management Journal",
        "S33741590": "Academy of Management Perspectives",  # ISSN: 1558-9080
        "S46763546": "Academy of Management Review",
        "S143668711": "Administrative Science Quarterly",
        "S172782825": "California Management Review",  # ISSN: 0008-1256
        "S65924262": "Contemporary Accounting Research",
        "S187626162": "Entrepreneurship Theory and Practice",
        "S134094273": "Human Resource Management",  # ISSN: 0090-4848
        "S145507837": "Industrial and Corporate Change",  # ISSN: 0960-6491
        "S202812398": "Information Systems Research",
        "S111116695": "Journal of Accounting Research",
        "S62142384": "Journal of Accounting and Economics",
        "S76633192": "Journal of Business Ethics",
        "S93284759": "Journal of Business Research",  # ISSN: 0148-2963
        "S66201313": "Journal of Business Venturing",
        "S145429826": "Journal of Consumer Research",
        "S5353659": "Journal of Finance",
        "S149240962": "Journal of Financial Economics",
        "S38024979": "Journal of International Business Studies",
        "S122767448": "Journal of Management",
        "S151705444": "Journal of Management Studies",
        "S142990027": "Journal of Marketing",
        "S119950638": "Journal of Marketing Research",
        "S142306484": "Journal of Operations Management",
        "S93630570": "Journal of Product Innovation Management",
        "S159120381": "Journal of Retailing",
        "S125608309": "Journal of Service Research",
        "S78792899": "Journal of Small Business Management",
        "S97320426": "Journal of Supply Chain Management",
        "S143995394": "Journal of World Business",  # ISSN: 1090-9516
        "S92522684": "Journal of the Academy of Marketing Science",
        "S9435936": "Long Range Planning",  # ISSN: 0024-6301
        "S57293258": "MIS Quarterly",
        "S33323087": "Management Science",
        "S163534328": "Marketing Science",
        "S125775545": "Operations Research",
        "S206124708": "Organization Science",
        "S28882882": "Organization Studies",  # ISSN: 0170-8406
        "S149070780": "Production and Operations Management",
        "S9731383": "Research Policy",  # ISSN: 0048-7333
        "S11853582": "Review of Accounting Studies",
        "S170137484": "Review of Financial Studies",
        "S102949365": "Strategic Management Journal",
        "S160506855": "The Accounting Review",
    },

}


# ISSN mapping for Crossref fallback
JOURNAL_ISSNS = {
    "Academy of Management Perspectives": "1558-9080",
    "Academy of Management Annals": "1941-6067",
    "Organization Studies": "0170-8406",
    "Human Resource Management": "0090-4848",
    "Journal of World Business": "1090-9516",
    "Long Range Planning": "0024-6301",
    "Research Policy": "0048-7333",
    "Journal of Business Research": "0148-2963",
    "Industrial and Corporate Change": "0960-6491",
    "California Management Review": "0008-1256",
    "The International Journal of Press/Politics": "1040-1620",
    "Communication Monographs": "0363-7751",
    "Annals of the International Communication Association": "2380-8977",
    "Journal of Computer-Mediated Communication": "1083-6101",
    "Social Media + Society": "2056-3051",
    "Journalism": "1464-8849",
    "Media Psychology": "1521-3269",
    "Communication Methods and Measures": "1931-2458",
    "Journal of Health Communication": "1081-0730",
    "Journal of Applied Econometrics": "0883-7252",
    "Oxford Bulletin of Economics and Statistics": "0305-9049",
    "Brookings Papers on Economic Activity": "0007-2303",
    "IMF Economic Review": "2041-4161",
    "World Bank Economic Review": "0258-6770",
    "Economic Policy": "0266-4658",
    "Review of Economic Dynamics": "1094-2025",
    "Journal of Economic Dynamics and Control": "0165-1889",
    "International Economic Review": "0020-6598",
    "British Educational Research Journal": "0141-1926",
    "Oxford Review of Education": "0305-4985",
    "Comparative Education Review": "0010-4086",
    "International Journal of Educational Research": "0883-0355",
    "Educational Administration Quarterly": "0013-161X",
    "Journal of Research on Educational Effectiveness": "1934-5739",
    "Science Education": "0036-8326",
    "Journal of Research in Science Teaching": "0022-4308",
    "Reading Research Quarterly": "0034-0553",
    "Journal of Literacy Research": "1086-296X",
    "Mathematics Education Research Journal": "1033-2170",
    "Cornell Law Review": "0010-8847",
    "Georgetown Law Journal": "0016-8092",
    "University of Chicago Law Review": "0041-9494",
    "Boston University Law Review": "0006-8047",
    "Minnesota Law Review": "0026-5535",
    "American Journal of Comparative Law": "0002-919X",
    "Modern Law Review": "0026-7961",
    "Oxford Journal of Legal Studies": "0143-6503",
    "Legal Theory": "1352-3252",
    "Law and Philosophy": "0167-5249",
    "Journal of Law and Economics": "2963-7937",
    "Journal of Legal Analysis": "1946-5319",
    "American Criminal Law Review": "0164-0364",
    "Journal of Criminal Law and Criminology": "0091-4169",
    "Cognitive Psychology": "0010-0285",
    "Memory & Cognition": "0090-502X",
    "Psychonomic Bulletin & Review": "1069-9384",
    "Journal of Experimental Child Psychology": "0022-0965",
    "Developmental Science": "1363-755X",
    "Infant Behavior and Development": "0163-6383",
    "Psychology and Aging": "0882-7974",
    "Neuropsychologia": "0028-3932",
    "Cortex": "0010-9452",
    "American Review of Public Administration": "0275-0740",
    "Journal of Comparative Policy Analysis": "1387-6988",
    "Population Studies": "0032-4728",
    "Sociological Forum": "0884-8971",
    "Sociological Quarterly": "0038-0253",
    "Sociological Perspectives": "0731-1214",
    "Social Networks": "0378-8733",
    "Poetics": "0304-422X",
    "Journal of Ethnic and Migration Studies": "1369-183X",
    "International Migration Review": "0197-9183",
    "Sociology of Religion": "1069-4404",
}
