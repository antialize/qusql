SELECT  booking_status_code ,  COUNT(*) FROM Apartment_Bookings GROUP BY booking_status_code
SELECT  T1.course_name FROM Courses AS T1 JOIN Student_Enrolment_Courses AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_name ORDER BY count(*) DESC LIMIT 1
SELECT  T1.customer_name ,  T1.customer_phone FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id JOIN order_items AS T3 ON T3.order_id  =  T2.order_id GROUP BY T1.customer_id ORDER BY sum(T3.order_quantity) DESC LIMIT 1
SELECT  T1.fname ,  T1.lname ,  count(*) ,  T1.FacID FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID GROUP BY T1.FacID
SELECT  T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1
SELECT * FROM  departments WHERE department_name  =  'Marketing'
SELECT * FROM  job_history AS T1 JOIN employees AS T2 ON T1.employee_id  =  T2.employee_id WHERE T2.salary  >=  12000
SELECT * FROM Albums WHERE YEAR  =  2010
SELECT * FROM Albums WHERE YEAR  =  2012
SELECT * FROM Assessment_Notes ORDER BY date_of_notes ASC
SELECT * FROM COURSE ORDER BY Credits
SELECT * FROM CUSTOMER WHERE State  =  \"NY\"
SELECT * FROM College ORDER BY enr
SELECT * FROM Course_Authors_and_Tutors ORDER BY personal_name
SELECT * FROM Student_Addresses ORDER BY monthly_rental DESC
SELECT * FROM airport ORDER BY International_Passengers DESC LIMIT 1
SELECT * FROM basketball_match
SELECT * FROM body_builder
SELECT * FROM chip_model
SELECT * FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" AND IsOfficial  =  \"T\" UNION SELECT * FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"Dutch\" AND IsOfficial  =  \"T\"
SELECT * FROM customer_master_index ORDER BY cmi_details DESC
SELECT * FROM election
SELECT * FROM employees WHERE department_id  =  \"null\"
SELECT * FROM employees WHERE department_id NOT IN (SELECT department_id FROM departments WHERE manager_id BETWEEN 100 AND 200)
SELECT * FROM employees WHERE employee_id NOT IN (SELECT employee_id FROM job_history)
SELECT * FROM employees WHERE first_name LIKE '%D%' OR first_name LIKE '%S%' ORDER BY salary DESC
SELECT * FROM employees WHERE hire_date  <  '2002-06-21'
SELECT * FROM employees WHERE hire_date  >  '1987-09-07'
SELECT * FROM employees WHERE salary BETWEEN (SELECT MIN(salary) FROM employees) AND 2500
SELECT * FROM employees WHERE salary BETWEEN 8000 AND 12000 AND commission_pct != \"null\" OR  department_id != 40
SELECT * FROM hiring
SELECT * FROM instructor ORDER BY salary
SELECT * FROM item
SELECT * FROM people
SELECT * FROM products AS T1 JOIN Manufacturers AS T2 ON T1.manufacturer  =  T2.code
SELECT * FROM products WHERE price BETWEEN 60 AND 120
SELECT * FROM student_course_registrations WHERE student_id NOT IN (SELECT student_id FROM student_course_attendance)
SELECT * FROM swimmer
SELECT AVG(Milliseconds) FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId WHERE T1.Name  =  \"Latin\" OR T1.Name  =  \"Pop\"
SELECT AVG(Price) ,  AVG(Cases) FROM WINE WHERE YEAR  =  2009 AND Grape  =  \"Zinfandel\"
SELECT AVG(Price) ,  Manufacturer FROM Products GROUP BY Manufacturer
SELECT AVG(T2.Price) FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T1.County  =  \"Sonoma\"
SELECT AVG(T2.UnitPrice) FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId WHERE T1.Name  =  \"Rock\"
SELECT AVG(UnitPrice) FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId WHERE T1.Name  =  \"Jazz\"
SELECT AVG(UnitPrice) FROM TRACK
SELECT Abbreviation FROM AIRLINES WHERE Airline  =  \"JetBlue Airways\"
SELECT Accreditation_level FROM phone GROUP BY Accreditation_level HAVING count(*)  >  3
SELECT Accreditation_type ,  count(*) FROM phone GROUP BY Accreditation_type
SELECT Address FROM LOCATIONS WHERE Location_Name  =  \"UK Gallery\"
SELECT Advisor ,  count(*) FROM STUDENT GROUP BY Advisor
SELECT Advisor FROM STUDENT GROUP BY Advisor HAVING COUNT(*)  >  2
SELECT Advisor FROM STUDENT GROUP BY Advisor HAVING count(*)  >  2
SELECT Advisor FROM STUDENT WHERE city_code  =  \"BAL\"
SELECT Advisor FROM Student WHERE StuID  =  1004
SELECT Age ,  COUNT(*) FROM editor GROUP BY Age
SELECT Age ,  Hometown FROM teacher
SELECT Age FROM artist
SELECT Age FROM editor GROUP BY Age ORDER BY COUNT(*) DESC LIMIT 1
SELECT Age FROM people ORDER BY Height DESC LIMIT 1
SELECT Aircraft FROM aircraft WHERE Aircraft_ID NOT IN (SELECT Winning_Aircraft FROM MATCH)
SELECT Airline ,  Abbreviation FROM AIRLINES WHERE Country  =  \"USA\"
SELECT Airline FROM AIRLINES WHERE Abbreviation  =  \"UAL\"
SELECT AirportCode ,  AirportName FROM AIRPORTS WHERE city  =  \"Anthony\"
SELECT AirportName FROM AIRPORTS WHERE AirportCode  =  \"AKO\"
SELECT AirportName FROM AIRPORTS WHERE City = \"Aberdeen\"
SELECT AirportName FROM Airports WHERE AirportCode NOT IN (SELECT SourceAirport FROM Flights UNION SELECT DestAirport FROM Flights)
SELECT Allergy ,  count(*) FROM Has_allergy GROUP BY Allergy
SELECT Allergy FROM Has_allergy GROUP BY Allergy ORDER BY count(*) DESC LIMIT 1
SELECT Amount_Settled ,  Amount_Claimed FROM Claims ORDER BY Amount_Claimed DESC LIMIT 1
SELECT Amount_Settled ,  Amount_Claimed FROM Claims ORDER BY Amount_Settled ASC LIMIT 1
SELECT Appelation FROM WINE WHERE YEAR  >  2008 EXCEPT SELECT Appelation FROM APPELLATIONS WHERE Area  =  \"Central Coast\"
SELECT Area ,  County FROM APPELLATIONS
SELECT Attendance FROM performance WHERE LOCATION  =  \"TD Garden\" OR LOCATION  =  \"Bell Centre\"
SELECT Author ,  College FROM submission
SELECT Author FROM submission ORDER BY Scores ASC
SELECT Author FROM submission ORDER BY Scores DESC LIMIT 1
SELECT Author FROM submission WHERE College  =  \"Florida\" OR College  =  \"Temple\"
SELECT Author FROM submission WHERE Submission_ID NOT IN (SELECT Submission_ID FROM acceptance)
SELECT BirthDate FROM EMPLOYEE WHERE City  =  \"Edmonton\"
SELECT Birth_Date FROM People ORDER BY Height ASC
SELECT Birth_Place ,  COUNT(*) FROM people GROUP BY Birth_Place
SELECT Birth_Place FROM people GROUP BY Birth_Place HAVING COUNT(*)  >=  2
SELECT Birth_Place FROM people GROUP BY Birth_Place ORDER BY COUNT(*) DESC LIMIT 1
SELECT Birth_Year ,  Citizenship FROM singer
SELECT Builder ,  COUNT(*) FROM railway GROUP BY Builder
SELECT Builder FROM railway GROUP BY Builder ORDER BY COUNT(*) DESC LIMIT 1
SELECT Builder FROM railway ORDER BY Builder ASC
SELECT CLASS ,  count(*) FROM race GROUP BY CLASS
SELECT CLASS FROM captain GROUP BY CLASS HAVING count(*)  >  2
SELECT CLASS FROM race GROUP BY CLASS HAVING count(*)  >=  2
SELECT CLASS FROM race GROUP BY CLASS ORDER BY count(*) DESC LIMIT 1
SELECT CName FROM COURSE ORDER BY Credits
SELECT CName FROM COURSE WHERE Credits  =  1
SELECT CName FROM COURSE WHERE Credits  =  3 UNION SELECT CName FROM COURSE WHERE Credits  =  1 AND Hours  =  4
SELECT CName FROM COURSE WHERE Days  =  \"MTW\"
SELECT COUNT (DISTINCT ID) FROM teaches WHERE semester  =  'Spring' AND YEAR  =  2010
SELECT COUNT (DISTINCT Publication_Date) FROM publication
SELECT COUNT (DISTINCT church_id) FROM wedding WHERE YEAR  =  2016
SELECT COUNT (DISTINCT director) FROM movie
SELECT COUNT (DISTINCT id) FROM teaches
SELECT COUNT (DISTINCT team) FROM elimination
SELECT COUNT( DISTINCT Continent) FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"Chinese\"
SELECT COUNT(*) FROM (SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" INTERSECT SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"Dutch\")
SELECT COUNT(*) FROM (SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'mid' EXCEPT SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'goalie')
SELECT COUNT(*) FROM (SELECT cName FROM  tryout WHERE pPos  =  'goalie' INTERSECT SELECT cName FROM  tryout WHERE pPos  =  'mid')
SELECT COUNT(*) FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistId WHERE T2.Name  =  \"Metallica\"
SELECT COUNT(*) FROM CUSTOMER WHERE Email LIKE \"%gmail.com%\"
SELECT COUNT(*) FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId WHERE T1.Name  =  \"Rock\"
SELECT COUNT(*) FROM MEDIATYPE AS T1 JOIN TRACK AS T2 ON T1.MediaTypeId  =  T2.MediaTypeId WHERE T1.Name  =  \"AAC audio file\"
SELECT COUNT(*) FROM Students WHERE personal_name LIKE \"%son%\"
SELECT COUNT(*) FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.Name  =  \"Afghanistan\" AND IsOfficial  =  \"T\"
SELECT COUNT(*) FROM orchestra WHERE Major_Record_Format  =  \"CD\" OR Major_Record_Format  =  \"DVD\"
SELECT COUNT(*) FROM station WHERE city  =  \"Mountain View\"
SELECT COUNT(*) FROM weather WHERE mean_humidity  >  50 AND mean_visibility_miles  >  8
SELECT COUNT(DISTINCT Artist_ID) FROM volume
SELECT COUNT(DISTINCT Country) FROM climber
SELECT COUNT(DISTINCT StuID) FROM ENROLLED_IN WHERE Grade  =  \"B\"
SELECT COUNT(DISTINCT city) FROM EMPLOYEE
SELECT COUNT(DISTINCT transaction_type_code) FROM TRANSACTIONS
SELECT COUNT(T2.Language) ,  T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode GROUP BY T1.Name HAVING COUNT(*)  >  2
SELECT COUNT(T2.Language) FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.Name  =  \"Aruba\"
SELECT Candidate_ID FROM candidate ORDER BY oppose_rate LIMIT 1
SELECT Carrier ,  COUNT(*) FROM phone GROUP BY Carrier
SELECT Carrier FROM device ORDER BY Carrier ASC
SELECT Carrier FROM device WHERE Device_ID NOT IN (SELECT Device_ID FROM stock)
SELECT Carrier FROM device WHERE Software_Platform != 'Android'
SELECT Carrier FROM phone GROUP BY Carrier ORDER BY COUNT(*) DESC LIMIT 1
SELECT Carrier FROM phone ORDER BY Price DESC LIMIT 1
SELECT Carrier FROM phone WHERE Memory_in_G  <  32 INTERSECT SELECT Carrier FROM phone WHERE Memory_in_G  >  64
SELECT Case_burden FROM county_public_safety ORDER BY Population DESC
SELECT Category ,  COUNT(*) FROM music_festival GROUP BY Category
SELECT Category FROM music_festival GROUP BY Category HAVING COUNT(*)  >  1
SELECT Category FROM music_festival WHERE RESULT  =  \"Awarded\"
SELECT Census_Ranking FROM city WHERE Status !=  \"Village\"
SELECT Chair_Name FROM festival_detail ORDER BY YEAR ASC
SELECT Character ,  Duration FROM actor
SELECT Character FROM actor ORDER BY age DESC
SELECT Citizenship ,  COUNT(*) FROM singer GROUP BY Citizenship
SELECT Citizenship ,  max(Net_Worth_Millions) FROM singer GROUP BY Citizenship
SELECT Citizenship FROM singer GROUP BY Citizenship ORDER BY COUNT(*) DESC LIMIT 1
SELECT Citizenship FROM singer WHERE Birth_Year  <  1945 INTERSECT SELECT Citizenship FROM singer WHERE Birth_Year  >  1955
SELECT City ,  Country FROM AIRPORTS WHERE AirportName  =  \"Alton\"
SELECT Clean_Jerk FROM body_builder ORDER BY Total DESC LIMIT 1
SELECT Code FROM country WHERE GovernmentForm != \"Republic\" EXCEPT SELECT CountryCode FROM countrylanguage WHERE LANGUAGE  =  \"English\"
SELECT College ,  COUNT(*) FROM submission GROUP BY College
SELECT College FROM match_season GROUP BY College HAVING count(*)  >=  2
SELECT College FROM match_season GROUP BY College HAVING count(*)  >=  2 ORDER BY College DESC
SELECT College FROM match_season GROUP BY College ORDER BY count(*) DESC LIMIT 3
SELECT College FROM match_season WHERE POSITION  =  \"Midfielder\" INTERSECT SELECT College FROM match_season WHERE POSITION  =  \"Defender\"
SELECT College FROM submission GROUP BY College ORDER BY COUNT(*) DESC LIMIT 1
SELECT College FROM submission WHERE Scores  >  90 INTERSECT SELECT College FROM submission WHERE Scores  <  80
SELECT Company ,  Investor FROM entrepreneur
SELECT Company FROM entrepreneur ORDER BY Money_Requested DESC
SELECT Competition_type ,  COUNT(*) FROM competition GROUP BY Competition_type
SELECT Competition_type ,  Country FROM competition
SELECT Competition_type FROM competition GROUP BY Competition_type HAVING COUNT(*)  <=  5
SELECT Competition_type FROM competition GROUP BY Competition_type ORDER BY COUNT(*) DESC LIMIT 1
SELECT Comptroller ,  COUNT(*) FROM party GROUP BY Comptroller ORDER BY COUNT(*) DESC LIMIT 1
SELECT Continent FROM country WHERE Name  =  \"Anguilla\"
SELECT Country ,  COUNT(*) FROM climber GROUP BY Country
SELECT Country ,  COUNT(*) FROM member GROUP BY Country
SELECT Country ,  COUNT(*) FROM perpetrator GROUP BY Country
SELECT Country ,  COUNT(*) FROM perpetrator GROUP BY Country HAVING COUNT(*)  >=  2
SELECT Country ,  COUNT(*) FROM perpetrator GROUP BY Country ORDER BY COUNT(*) DESC LIMIT 1
SELECT Country FROM AIRLINES WHERE Airline  =  \"JetBlue Airways\"
SELECT Country FROM manager WHERE Age  >  50 INTERSECT SELECT Country FROM manager WHERE Age  <  46
SELECT Country FROM manager WHERE Age  >  50 OR Age  <  46
SELECT Country FROM market ORDER BY Country ASC
SELECT Country FROM market ORDER BY Number_cities DESC
SELECT Country FROM member GROUP BY Country HAVING COUNT(*)  >  2
SELECT Country FROM member GROUP BY Country ORDER BY COUNT(*) DESC LIMIT 1
SELECT Country FROM mountain GROUP BY Country HAVING COUNT(*)  >  1
SELECT Country FROM mountain WHERE Height  >  5000
SELECT Country FROM mountain WHERE Height  >  5600 INTERSECT SELECT Country FROM mountain WHERE Height  <  5200
SELECT Country FROM perpetrator WHERE Injured  >  50 INTERSECT SELECT Country FROM perpetrator WHERE Injured  <  20
SELECT CountryCode ,  max(Percentage) FROM countrylanguage WHERE LANGUAGE  =  \"Spanish\" GROUP BY CountryCode
SELECT CountryCode FROM countrylanguage EXCEPT SELECT CountryCode FROM countrylanguage WHERE LANGUAGE  =  \"English\"
SELECT Country_name ,  COUNT(*) FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country GROUP BY T1.Country_name
SELECT Country_name ,  Capital FROM country
SELECT County_name ,  Population FROM county
SELECT County_name FROM county ORDER BY County_name DESC
SELECT County_name FROM county ORDER BY Population ASC
SELECT County_name FROM county ORDER BY Population ASC LIMIT 3
SELECT County_name FROM county ORDER BY Population DESC LIMIT 1
SELECT Crime_rate FROM county_public_safety ORDER BY Police_officers ASC
SELECT Customer_Name FROM Clients EXCEPT SELECT T2.Customer_Name FROM Bookings AS T1 JOIN Clients AS T2 ON T1.Customer_ID  =  T2.Client_ID
SELECT Customer_Phone ,  Customer_Email_Address FROM CUSTOMERS WHERE Customer_Name  =  \"Harold\"
SELECT Customer_Phone FROM PERFORMERS WHERE Customer_Name  =  \"Ashley\"
SELECT DISTINCT * FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id WHERE T1.employee_id  =  T2.manager_id
SELECT DISTINCT CLASS FROM race
SELECT DISTINCT Carrier FROM phone WHERE Memory_in_G  >  32
SELECT DISTINCT Country FROM manager
SELECT DISTINCT CountryCode FROM countrylanguage WHERE LANGUAGE != \"English\"
SELECT DISTINCT Destroyed_by_Employee_ID FROM Documents_to_be_destroyed
SELECT DISTINCT Destruction_Authorised_by_Employee_ID FROM Documents_to_be_destroyed
SELECT DISTINCT Director FROM film
SELECT DISTINCT District FROM election
SELECT DISTINCT District_name FROM district ORDER BY city_area DESC
SELECT DISTINCT Fname FROM STUDENT WHERE Fname LIKE '%a%'
SELECT DISTINCT Hometown FROM people EXCEPT SELECT DISTINCT T2.Hometown FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID
SELECT DISTINCT LOCATION FROM cinema
SELECT DISTINCT LOCATION FROM station
SELECT DISTINCT LOCATION FROM station WHERE number_of_platforms  >=  15 AND total_passengers  >  25
SELECT DISTINCT LOCATION FROM track
SELECT DISTINCT Location_Name FROM LOCATIONS
SELECT DISTINCT Major FROM Student
SELECT DISTINCT Name FROM WINE ORDER BY Name
SELECT DISTINCT Name FROM WINE ORDER BY price
SELECT DISTINCT Name FROM WINE WHERE Price  >  (SELECT min(Price) FROM wine WHERE Winery  =  \"John Anthony\")
SELECT DISTINCT Name FROM WINE WHERE YEAR  <  2000 OR YEAR  >  2010
SELECT DISTINCT Nominee FROM musical WHERE Award != \"Tony Award\"
SELECT DISTINCT POSITION FROM match_season
SELECT DISTINCT POSITION FROM match_season WHERE College  =  \"UCLA\" OR College  =  \"Duke\"
SELECT DISTINCT POSITION FROM pilot WHERE Age  >  30
SELECT DISTINCT PRESIDENT_Vote FROM VOTING_RECORD WHERE Registration_Date  =  \"08/30/2015\"
SELECT DISTINCT Police_force FROM county_public_safety WHERE LOCATION != \"East\"
SELECT DISTINCT President_Vote ,  VICE_President_Vote FROM VOTING_RECORD
SELECT DISTINCT Product_Name FROM PRODUCTS WHERE Product_Price  >  (SELECT avg(Product_Price) FROM PRODUCTS)
SELECT DISTINCT Publisher FROM publication WHERE Price  >  5000000
SELECT DISTINCT Range FROM mountain ORDER BY Prominence DESC LIMIT 3
SELECT DISTINCT Region FROM club ORDER BY Region ASC
SELECT DISTINCT Registration_Date ,  Election_Cycle FROM VOTING_RECORD
SELECT DISTINCT Reign FROM wrestler WHERE LOCATION != \"Tokyo , Japan\"
SELECT DISTINCT Secretary_Vote FROM VOTING_RECORD WHERE ELECTION_CYCLE  =  \"Fall\"
SELECT DISTINCT T1.Address FROM CUSTOMER AS T1 JOIN INVOICE AS T2 ON T1.CustomerId  =  T2.CustomerId WHERE T1.country  =  \"Germany\"
SELECT DISTINCT T1.Advisor FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.Treasurer_Vote WHERE T2.Election_Cycle  =  \"Spring\"
SELECT DISTINCT T1.Age FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.Secretary_Vote WHERE T2.Election_Cycle  =  \"Fall\"
SELECT DISTINCT T1.Director FROM film AS T1 JOIN film_market_estimation AS T2 ON T1.Film_ID  =  T2.Film_ID WHERE T2.Year  =  1995
SELECT DISTINCT T1.EMP_FNAME ,  T1.EMP_DOB FROM employee AS T1 JOIN CLASS AS T2 ON T1.EMP_NUM  =  T2.PROF_NUM WHERE CRS_CODE  =  \"ACCT-211\"
SELECT DISTINCT T1.Fate FROM mission AS T1 JOIN ship AS T2 ON T1.Ship_ID  =  T2.Ship_ID WHERE T2.Nationality  =  \"United States\"
SELECT DISTINCT T1.FirstName FROM CUSTOMER AS T1 JOIN INVOICE AS T2 ON T1.CustomerId  =  T2.CustomerId WHERE T1.country  =  \"Brazil\"
SELECT DISTINCT T1.Fname ,  T1.LName FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.President_VOTE WHERE T1.sex  =  \"F\"
SELECT DISTINCT T1.Fname ,  T1.LName FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.VICE_President_VOTE WHERE T1.age  =  18
SELECT DISTINCT T1.Fname FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.CLASS_Senator_VOTE
SELECT DISTINCT T1.Fname FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.VICE_PRESIDENT_Vote EXCEPT SELECT DISTINCT Fname FROM STUDENT WHERE city_code  =  \"PIT\"
SELECT DISTINCT T1.Fname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat' OR T3.pettype  =  'dog'
SELECT DISTINCT T1.Grape FROM GRAPES AS T1 JOIN WINE AS T2 ON T1.Grape  =  T2.Grape WHERE T1.Color  =  \"White\" AND T2.score  >  90
SELECT DISTINCT T1.LName FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  PRESIDENT_Vote EXCEPT SELECT DISTINCT LName FROM STUDENT WHERE Advisor  =  \"2192\"
SELECT DISTINCT T1.LName FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  PRESIDENT_Vote INTERSECT SELECT DISTINCT LName FROM STUDENT WHERE Advisor  =  \"8741\"
SELECT DISTINCT T1.LName FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.CLASS_President_VOTE
SELECT DISTINCT T1.Leader_Name FROM college AS T1 JOIN member AS T2 ON T1.College_ID  =  T2.College_ID WHERE T2.Country  =  \"Canada\"
SELECT DISTINCT T1.Major FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  T2.Treasurer_Vote
SELECT DISTINCT T1.Name FROM singer AS T1 JOIN song AS T2 ON T1.Singer_ID  =  T2.Singer_ID WHERE T2.Sales  >  300000
SELECT DISTINCT T1.Region FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" OR T2.Language  =  \"Dutch\"
SELECT DISTINCT T1.age FROM management AS T2 JOIN head AS T1 ON T1.head_id  =  T2.head_id WHERE T2.temporary_acting  =  'Yes'
SELECT DISTINCT T1.artist_name ,  T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.rating  >  9
SELECT DISTINCT T1.bname FROM bank AS T1 JOIN loan AS T2 ON T1.branch_id  =  T2.branch_id
SELECT DISTINCT T1.city FROM addresses AS T1 JOIN people_addresses AS T2 ON T1.address_id = T2.address_id
SELECT DISTINCT T1.city FROM addresses AS T1 JOIN people_addresses AS T2 ON T1.address_id = T2.address_id JOIN students AS T3 ON T2.person_id = T3.student_id
SELECT DISTINCT T1.course_name FROM Courses AS T1 JOIN Student_Enrolment_Courses AS T2 ON T1.course_id  =  T2.course_id
SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T3.born_state  =  'Alabama'
SELECT DISTINCT T1.cust_name ,  T1.credit_score FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id
SELECT DISTINCT T1.customer_details FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"On Road\"
SELECT DISTINCT T1.customer_first_name ,  T1.customer_last_name ,  T1.phone_number FROM Customers AS T1 JOIN Accounts AS T2 ON T1.customer_id  =  T2.customer_id
SELECT DISTINCT T1.customer_first_name ,  T1.customer_last_name FROM Customers AS T1 JOIN Accounts AS T2 ON T1.customer_id  =  T2.customer_id
SELECT DISTINCT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id JOIN order_items AS T3 ON T2.order_id  =  T3.order_id GROUP BY T1.customer_id HAVING COUNT (DISTINCT T3.product_id)  >=  3
SELECT DISTINCT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id JOIN order_items AS T3 ON T2.order_id  =  T3.order_id JOIN products AS T4 ON T3.product_id  =  T4.product_id WHERE T4.product_name  =  \"keyboard\"
SELECT DISTINCT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.order_status_code  =  \"Pending\" ORDER BY T2.customer_id
SELECT DISTINCT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"Packing\"
SELECT DISTINCT T1.date_arrived ,  T1.date_departed FROM Dogs AS T1 JOIN Treatments AS T2 ON T1.dog_id  =  T2.dog_id
SELECT DISTINCT T1.driverid ,  T1.nationality FROM drivers AS T1 JOIN laptimes AS T2 ON T1.driverid = T2.driverid WHERE T2.milliseconds >  100000
SELECT DISTINCT T1.file_size ,  T1.formats FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T2.resolution  <  800
SELECT DISTINCT T1.first_name ,  T1.last_name FROM staff AS T1 JOIN rental AS T2 ON T1.staff_id  =  T2.staff_id JOIN customer AS T3 ON T2.customer_id  =  T3.customer_id WHERE T3.first_name  =  'APRIL' AND T3.last_name  =  'BURNS'
SELECT DISTINCT T1.first_name ,  T1.middle_name ,  T1.last_name FROM Students AS T1 JOIN Student_Enrolment AS T2 ON T1.student_id  =  T2.student_id JOIN Degree_Programs AS T3 ON T2.degree_program_id  =  T3.degree_program_id WHERE T3.degree_summary_name  =  'Bachelor'
SELECT DISTINCT T1.first_name ,  T3.treatment_type_description FROM professionals AS T1 JOIN Treatments AS T2 ON T1.professional_id  =  T2.professional_id JOIN Treatment_types AS T3 ON T2.treatment_type_code  =  T3.treatment_type_code
SELECT DISTINCT T1.firstname ,  T1.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.grade  =  1 EXCEPT SELECT T1.firstname ,  T1.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"OTHA\" AND T2.lastname  =  \"MOYER\"
SELECT DISTINCT T1.fname ,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid
SELECT DISTINCT T1.fname ,  T1.city_code FROM Student AS T1 JOIN Has_Allergy AS T2 ON T1.stuid  =  T2.stuid WHERE T2.Allergy  =  \"Milk\" OR T2.Allergy  =  \"Cat\"
SELECT DISTINCT T1.forename ,  T1.surname FROM drivers AS T1 JOIN laptimes AS T2 ON T1.driverid = T2.driverid WHERE T2.milliseconds < 93000
SELECT DISTINCT T1.forename FROM drivers AS T1 JOIN driverstandings AS T2 ON T1.driverid = T2.driverid WHERE T2.position = 1 AND T2.wins = 1
SELECT DISTINCT T1.forename FROM drivers AS T1 JOIN driverstandings AS T2 ON T1.driverid = T2.driverid WHERE T2.position = 1 AND T2.wins = 1 AND T2.points > 20
SELECT DISTINCT T1.id ,  T1.name FROM battle AS T1 JOIN ship AS T2 ON T1.id  =  T2.lost_in_battle WHERE T2.ship_type  =  'Brig'
SELECT DISTINCT T1.id ,  T1.name FROM station AS T1 JOIN status AS T2 ON T1.id  =  T2.station_id WHERE T2.bikes_available  >  12
SELECT DISTINCT T1.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.grade  =  3 AND T2.firstname != \"COVIN\" AND T2.lastname != \"JEROME\"
SELECT DISTINCT T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID JOIN activity AS T3 ON T2.actid  =  T2.actid WHERE T3.activity_name  =  'Canoeing' OR T3.activity_name  =  'Kayaking'
SELECT DISTINCT T1.name ,  T1.age FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend  =  'Dan' OR T2.friend  =  'Alice'
SELECT DISTINCT T1.name ,  T1.location ,  T1.product FROM enzyme AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.enzyme_id  =  T1.id WHERE T2.interaction_type  =  'inhibitor'
SELECT DISTINCT T1.name ,  T1.nationality FROM architect AS T1 JOIN mill AS t2 ON T1.id  =  T2.architect_id
SELECT DISTINCT T1.name ,  T1.position FROM physician AS T1 JOIN prescribes AS T2 ON T1.employeeid = T2.physician JOIN medication AS T3 ON T3.code = T2.medication WHERE T3.Brand  =  \"X\"
SELECT DISTINCT T1.name FROM mill AS T1 JOIN architect AS t2 ON T1.architect_id  =  T2.id JOIN bridge AS T3 ON T3.architect_id  =  T2.id WHERE T3.length_meters  >  80
SELECT DISTINCT T1.name FROM nurse AS T1 JOIN on_call AS T2 ON T1.EmployeeID  =  T2.nurse
SELECT DISTINCT T1.name FROM physician AS T1 JOIN prescribes AS T2 ON T1.employeeid = T2.physician JOIN medication AS T3 ON T3.code = T2.medication WHERE T3.name  =  \"Thesisin\"
SELECT DISTINCT T1.name FROM races AS T1 JOIN circuits AS T2 ON T1.circuitid = T2.circuitid WHERE T2.country = \"Spain\" AND T1.year > 2000
SELECT DISTINCT T1.name FROM station AS T1 JOIN status AS T2 ON T1.id  =  T2.station_id WHERE T2.bikes_available  =  7
SELECT DISTINCT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id  =  T2.id WHERE YEAR  =  2009 OR YEAR  =  2010
SELECT DISTINCT T1.player_name ,  T1.birthday FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id  =  T2.player_api_id ORDER BY potential DESC LIMIT 5
SELECT DISTINCT T1.player_name FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id  =  T2.player_api_id ORDER BY overall_rating DESC LIMIT 3
SELECT DISTINCT T1.player_name FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id WHERE T2.crossing  >  90 AND T2.preferred_foot  =  \"right\"
SELECT DISTINCT T1.player_name FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id WHERE T2.dribbling  =  ( SELECT max(overall_rating) FROM Player_Attributes)
SELECT DISTINCT T1.player_name FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id WHERE T2.overall_rating  >  ( SELECT avg(overall_rating) FROM Player_Attributes )
SELECT DISTINCT T1.player_name FROM Player AS T1 JOIN Player_Attributes AS T2 ON T1.player_api_id = T2.player_api_id WHERE T2.preferred_foot  =  \"left\" AND T2.overall_rating  >=  85 AND T2.overall_rating  <=  90
SELECT DISTINCT T1.service_type_code FROM services AS T1 JOIN organizations AS T2 ON T1.organization_id  =  T2.organization_id WHERE T2.organization_details  =  'Denesik and Sons Party'
SELECT DISTINCT T1.stageposition FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id WHERE Firstname  =  \"Solveig\"
SELECT DISTINCT T1.state ,  T1.enr FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.decision  =  'yes'
SELECT DISTINCT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.decision  =  'yes'
SELECT DISTINCT T1.supplier_name ,  T1.supplier_phone FROM suppliers AS T1 JOIN product_suppliers AS T2 ON T1.supplier_id  =  T2.supplier_id JOIN products AS T3 ON T2.product_id  =  T3.product_id WHERE T3.product_name  =  \"red jeans\"
SELECT DISTINCT T1.template_type_description FROM Ref_template_types AS T1 JOIN Templates AS T2 ON T1.template_type_code  = T2.template_type_code JOIN Documents AS T3 ON T2.Template_ID  =  T3.template_ID
SELECT DISTINCT T1.type FROM mill AS T1 JOIN architect AS t2 ON T1.architect_id  =  T2.id WHERE T2.nationality  =  'American' OR T2.nationality  =  'Canadian'
SELECT DISTINCT T1.type FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id WHERE T2.velocity  <  200
SELECT DISTINCT T2.Hardware_Model_name FROM screen_mode AS T1 JOIN phone AS T2 ON T1.Graphics_mode = T2.screen_mode WHERE T1.Type  =  \"Graphics\" OR t2.Company_name  =  \"Nokia Corporation\"
SELECT DISTINCT T2.Hometown FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID WHERE T1.Total_Points  >  57.5
SELECT DISTINCT T2.Name FROM APPELLATIONs AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T1.Area  =  \"North Coast\"
SELECT DISTINCT T2.Name FROM GRAPES AS T1 JOIN WINE AS T2 ON T1.Grape  =  T2.Grape WHERE T1.Color  =  \"Red\"
SELECT DISTINCT T2.Name FROM climber AS T1 JOIN mountain AS T2 ON T1.Mountain_ID  =  T2.Mountain_ID WHERE T1.Country  =  \"West Germany\"
SELECT DISTINCT T2.Name FROM country AS T1 JOIN city AS T2 ON T2.CountryCode  =  T1.Code WHERE T1.Continent  =  'Europe' AND T1.Name NOT IN (SELECT T3.Name FROM country AS T3 JOIN countrylanguage AS T4 ON T3.Code  =  T4.CountryCode WHERE T4.IsOfficial  =  'T' AND T4.Language  =  'English')
SELECT DISTINCT T2.Position FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T1.Capital  =  \"Dublin\"
SELECT DISTINCT T2.Type FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID WHERE T3.Year  =  2007
SELECT DISTINCT T2.apt_number FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T1.booking_status_code  =  \"Confirmed\"
SELECT DISTINCT T2.department_name FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id
SELECT DISTINCT T2.director FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID WHERE T3.name  =  'Sarah Martinez'
SELECT DISTINCT T2.emp_fname ,  T3.prof_high_degree FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num JOIN professor AS T3 ON T2.emp_num  =  T3.emp_num JOIN department AS T4 ON T4.dept_code  =  T3.dept_code WHERE T4.dept_name  =  'Computer Info. Systems'
SELECT DISTINCT T2.firstname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE grade  =  1
SELECT DISTINCT T2.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE grade  =  5
SELECT DISTINCT T2.name ,  T2.friend ,  T1.age FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T1.age  >  (SELECT avg(age) FROM person)
SELECT DISTINCT T2.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T1.age  <  (SELECT avg(age) FROM person)
SELECT DISTINCT T2.name FROM affiliated_with AS T1 JOIN department AS T2 ON T1.department = T2.departmentid WHERE PrimaryAffiliation  =  1
SELECT DISTINCT T2.name FROM undergoes AS T1 JOIN nurse AS T2 ON T1.AssistingNurse  =  T2.EmployeeID
SELECT DISTINCT T2.name FROM undergoes AS T1 JOIN patient AS T2 ON T1.patient = T2.SSN JOIN stay AS T3 ON T1.Stay  =  T3.StayID WHERE T3.room  =  111
SELECT DISTINCT T2.pilot FROM airport AS T1 JOIN flight AS T2 ON T1.id  =  T2.airport_id WHERE T1.country  =  'United States' OR T1.name  =  'Billund Airport'
SELECT DISTINCT T2.problem_id ,  T2.problem_log_id FROM staff AS T1 JOIN problem_log AS T2 ON T1.staff_id = T2.assigned_to_staff_id WHERE T1.staff_first_name = \"Rylan\" AND T1.staff_last_name = \"Homenick\"
SELECT DISTINCT T2.product_id FROM orders AS T1 JOIN order_items AS T2 ON T1.order_id = T2.order_id WHERE T1.date_order_placed >= \"1975-01-01\" AND T1.date_order_placed <= \"1976-01-01\"
SELECT DISTINCT T2.semester_id FROM Degree_Programs AS T1 JOIN Student_Enrolment AS T2 ON T1.degree_program_id  =  T2.degree_program_id WHERE degree_summary_name  =  'Master' INTERSECT SELECT DISTINCT T2.semester_id FROM Degree_Programs AS T1 JOIN Student_Enrolment AS T2 ON T1.degree_program_id  =  T2.degree_program_id WHERE degree_summary_name  =  'Bachelor'
SELECT DISTINCT T2.source_system_code FROM customer_master_index AS T1 JOIN cmi_cross_references AS T2 ON T1.master_customer_id  =  T2.master_customer_id WHERE T1.cmi_details  =  'Gottlieb ,  Becker and Wyman'
SELECT DISTINCT T2.thing_id ,  T2.Type_of_Thing_Code FROM Timed_Status_of_Things AS T1 JOIN Things AS T2 ON T1.thing_id  =  T2.thing_id WHERE T1.Status_of_Thing_Code  =  'Close' OR T1.Date_and_Date  <  '2017-06-19 02:59:21'
SELECT DISTINCT T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID WHERE T3.name  =  'Brittany Harris' OR T2.year  >  2000
SELECT DISTINCT T3.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode JOIN city AS T3 ON T1.Code  =  T3.CountryCode WHERE T2.IsOfficial  =  'T' AND T2.Language  =  'Chinese' AND T1.Continent  =  \"Asia\"
SELECT DISTINCT T3.name ,  T2.title ,  T1.stars FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID WHERE T2.director  =  T3.name
SELECT DISTINCT T3.name FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID WHERE T2.title  =  'Gone with the Wind'
SELECT DISTINCT T3.product_name FROM customer_orders AS T1 JOIN order_items AS T2 ON T1.order_id  =  T2.order_id JOIN products AS T3 ON T2.product_id  =  T3.product_id GROUP BY T3.product_id HAVING COUNT (DISTINCT T1.customer_id)  >=  2
SELECT DISTINCT T4.name FROM PersonFriend AS T1 JOIN Person AS T2 ON T1.name  =  T2.name JOIN PersonFriend AS T3 ON T1.friend  =  T3.name JOIN PersonFriend AS T4 ON T3.friend  =  T4.name WHERE T2.name  =  'Alice' AND T4.name != 'Alice'
SELECT DISTINCT TYPE FROM institution
SELECT DISTINCT TYPE FROM vocals
SELECT DISTINCT Theme FROM journal
SELECT DISTINCT Venue FROM debate
SELECT DISTINCT Visit_Date FROM VISITS
SELECT DISTINCT Winery FROM WINE WHERE Price BETWEEN 50 AND 100
SELECT DISTINCT YEAR FROM Movie AS T1 JOIN Rating AS T2 ON T1.mID  =  T2.mID WHERE T2.stars  >=  4 ORDER BY T1.year
SELECT DISTINCT YEAR FROM competition WHERE Competition_type != \"Tournament\"
SELECT DISTINCT YEAR FROM party WHERE Governor  =  \"Eliot Spitzer\"
SELECT DISTINCT address_type_code FROM customer_addresses
SELECT DISTINCT allergy FROM Allergy_type WHERE allergytype  =  \"food\"
SELECT DISTINCT allergytype FROM Allergy_type
SELECT DISTINCT artist_name FROM artist WHERE country  =  \"Bangladesh\" EXCEPT SELECT DISTINCT artist_name FROM song WHERE rating  >  7
SELECT DISTINCT artist_name FROM song WHERE languages  =  \"english\" EXCEPT SELECT DISTINCT artist_name FROM song WHERE rating  >  8
SELECT DISTINCT asset_model FROM Assets
SELECT DISTINCT blockcode FROM room WHERE unavailable  =  0
SELECT DISTINCT breed_code ,  size_code FROM dogs
SELECT DISTINCT building FROM Faculty
SELECT DISTINCT building FROM classroom WHERE capacity  >  50
SELECT DISTINCT building_description FROM Apartment_Buildings
SELECT DISTINCT cName FROM college WHERE enr  >  (SELECT min(enr) FROM college WHERE state  =  'FL')
SELECT DISTINCT cName FROM tryout ORDER BY cName
SELECT DISTINCT card_type_code FROM Customers_Cards
SELECT DISTINCT channel_code FROM customers AS t1 JOIN customer_contact_channels AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.customer_name  =  \"Tillman Ernser\"
SELECT DISTINCT characteristic_name FROM CHARACTERISTICS
SELECT DISTINCT city FROM addresses
SELECT DISTINCT city FROM branch WHERE membership_amount  >=  100
SELECT DISTINCT city_code FROM Student
SELECT DISTINCT classroom ,  grade FROM list
SELECT DISTINCT classroom FROM list WHERE grade  =  4
SELECT DISTINCT classroom FROM list WHERE grade  =  5
SELECT DISTINCT country FROM artist
SELECT DISTINCT country FROM singer WHERE age  >  20
SELECT DISTINCT customer_details FROM Customers
SELECT DISTINCT customer_id FROM Customer_Orders WHERE order_date  >  (SELECT min(order_date) FROM Customer_Orders WHERE order_status_code  =  \"Cancelled\")
SELECT DISTINCT date_moved_in FROM residents
SELECT DISTINCT department_id FROM employees GROUP BY department_id ,  manager_id HAVING COUNT(employee_id)  >= 4
SELECT DISTINCT detention_type_code FROM Detention
SELECT DISTINCT directed_by FROM film
SELECT DISTINCT donator_name FROM endowment
SELECT DISTINCT driverid ,  STOP FROM pitstops WHERE duration  <  (SELECT max(duration) FROM pitstops WHERE raceid  =  841)
SELECT DISTINCT driverid ,  STOP FROM pitstops WHERE duration  >  (SELECT min(duration) FROM pitstops WHERE raceid  =  841)
SELECT DISTINCT fault_status FROM Fault_Log_Parts
SELECT DISTINCT firstname ,  lastname FROM list
SELECT DISTINCT firstname ,  lastname FROM teachers
SELECT DISTINCT firstname FROM list WHERE classroom  =  107
SELECT DISTINCT fname FROM student
SELECT DISTINCT forename FROM drivers ORDER BY forename ASC
SELECT DISTINCT form_type_code FROM forms
SELECT DISTINCT grade FROM list WHERE classroom  =  103
SELECT DISTINCT grade FROM list WHERE classroom  =  105
SELECT DISTINCT gtype FROM Video_games
SELECT DISTINCT headquarter FROM manufacturers
SELECT DISTINCT home_city FROM driver
SELECT DISTINCT instrument FROM Instruments
SELECT DISTINCT invoice_details FROM invoices WHERE invoice_date < \"1989-09-03\" OR invoice_date > \"2007-12-25\"
SELECT DISTINCT label FROM Albums
SELECT DISTINCT last_name FROM Maintenance_Engineers
SELECT DISTINCT location_code FROM Document_locations
SELECT DISTINCT lot_details FROM LOTS
SELECT DISTINCT main_industry FROM company
SELECT DISTINCT name FROM  instructor ORDER BY name
SELECT DISTINCT name FROM Reviewer AS T1 JOIN Rating AS T2 ON T1.rID  =  T2.rID WHERE ratingDate  =  \"null\"
SELECT DISTINCT name FROM medication ORDER BY name
SELECT DISTINCT name FROM nurse ORDER BY name
SELECT DISTINCT name FROM races ORDER BY name DESC
SELECT DISTINCT name FROM races WHERE YEAR BETWEEN 2014 AND 2017
SELECT DISTINCT order_id FROM orders ORDER BY date_order_placed
SELECT DISTINCT pName FROM Player WHERE pName LIKE '%a%'
SELECT DISTINCT payment_method FROM customers
SELECT DISTINCT payment_method_code FROM INVOICES
SELECT DISTINCT payment_type_code FROM payments
SELECT DISTINCT premises_type FROM premises
SELECT DISTINCT product_color FROM Products
SELECT DISTINCT product_details FROM products
SELECT DISTINCT product_name FROM product ORDER BY product_id
SELECT DISTINCT product_name FROM product ORDER BY product_name
SELECT DISTINCT product_name FROM products ORDER BY product_name
SELECT DISTINCT product_size FROM Products
SELECT DISTINCT publisher FROM book_club
SELECT DISTINCT rank FROM Faculty
SELECT DISTINCT region_name FROM region ORDER BY Label
SELECT DISTINCT salary FROM instructor WHERE salary  <  (SELECT max(salary) FROM instructor)
SELECT DISTINCT song_name FROM song WHERE resolution  >  (SELECT min(resolution) FROM song WHERE languages  =  \"english\")
SELECT DISTINCT source_system_code FROM cmi_cross_references WHERE source_system_code LIKE '%en%'
SELECT DISTINCT staff_first_name ,  staff_last_name FROM staff AS T1 JOIN problem_log AS T2 ON T1.staff_id = T2.assigned_to_staff_id WHERE T2.problem_id = 1
SELECT DISTINCT start_station_name FROM trip WHERE duration  <  100
SELECT DISTINCT state ,  created FROM votes
SELECT DISTINCT state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName
SELECT DISTINCT state FROM college WHERE enr  <  (SELECT max(enr) FROM college)
SELECT DISTINCT stu_fname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num WHERE enroll_grade  =  'C'
SELECT DISTINCT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.advisor  =  1121
SELECT DISTINCT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.city_code  =  \"BAL\"
SELECT DISTINCT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.city_code  =  \"HOU\"
SELECT DISTINCT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.fname  =  \"Davis\" AND t3.lname  =  \"Steven\"
SELECT DISTINCT t1.fname ,  t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"Google\"
SELECT DISTINCT t1.fname ,  t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"University of Oxford\"
SELECT DISTINCT t1.individual_last_name FROM individuals AS t1 JOIN organization_contact_individuals AS t2 ON t1.individual_id  =  t2.individual_id
SELECT DISTINCT t1.product_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t3.characteristic_name  =  \"warm\"
SELECT DISTINCT t1.title FROM papers AS t1 JOIN authorship AS t2 ON t1.paperid  =  t2.paperid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"Google\"
SELECT DISTINCT t1.title FROM papers AS t1 JOIN authorship AS t2 ON t1.paperid  =  t2.paperid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"Indiana University\"
SELECT DISTINCT t2.customer_details FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.policy_type_code  =  \"Deputy\"
SELECT DISTINCT t2.customer_details FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.policy_type_code  =  \"Deputy\" OR t1.policy_type_code  =  \"Uniform\"
SELECT DISTINCT t3.name FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t1.fname  =  \"Katsuhiro\" AND t1.lname  =  \"Ueno\"
SELECT DISTINCT t3.name FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t1.fname  =  \"Matthias\" AND t1.lname  =  \"Blume\"
SELECT DISTINCT t3.policy_type_code FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id JOIN available_policies AS t3 ON t2.policy_id  =  t3.policy_id WHERE t1.customer_name  =  \"Dayana Robel\"
SELECT DISTINCT t3.policy_type_code FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id JOIN available_policies AS t3 ON t2.policy_id  =  t3.policy_id WHERE t1.customer_name  =  (SELECT t1.customer_name FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id GROUP BY t1.customer_name ORDER BY count(*) DESC LIMIT 1)
SELECT DISTINCT template_type_code FROM Templates
SELECT DISTINCT title FROM Movie EXCEPT SELECT T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID WHERE T3.name  =  'Chris Jackson'
SELECT DISTINCT title FROM vocals AS t1 JOIN songs AS t2 ON t1.songid  =  t2.songid EXCEPT SELECT t2.title FROM vocals AS t1 JOIN songs AS t2 ON t1.songid  =  t2.songid WHERE TYPE  =  \"back\"
SELECT DISTINCT title FROM vocals AS t1 JOIN songs AS t2 ON t1.songid  =  t2.songid EXCEPT SELECT t2.title FROM vocals AS t1 JOIN songs AS t2 ON t1.songid  =  t2.songid WHERE TYPE  =  \"lead\"
SELECT DISTINCT transaction_type FROM Financial_Transactions
SELECT DISTINCT winner_name ,  winner_rank FROM matches ORDER BY winner_age LIMIT 3
SELECT DISTINCT zip_code FROM weather EXCEPT SELECT DISTINCT zip_code FROM weather WHERE max_dew_point_f  >=  70
SELECT DName FROM DEPARTMENT EXCEPT SELECT T1.DName FROM DEPARTMENT AS T1 JOIN MINOR_IN AS T2 ON T1.DNO  =  T2.DNO
SELECT DName FROM DEPARTMENT WHERE Building  =  \"Mergenthaler\"
SELECT DName FROM DEPARTMENT WHERE Division  =  \"AS\" UNION SELECT DName FROM DEPARTMENT WHERE Division  =  \"EN\" AND Building  =  \"NEB\"
SELECT DPhone FROM DEPARTMENT WHERE Room  =  268
SELECT Date ,  LOCATION FROM performance
SELECT Date ,  Name ,  venue FROM event ORDER BY Event_Attendance DESC
SELECT Date ,  Venue FROM debate
SELECT Date ,  Venue FROM workshop ORDER BY Venue
SELECT Date ,  Vote_Percent FROM election
SELECT Date FROM debate WHERE Num_of_Audience  >  150
SELECT Date FROM game ORDER BY home_team DESC
SELECT Date FROM performance ORDER BY Attendance DESC LIMIT 1
SELECT Date FROM perpetrator ORDER BY Killed DESC
SELECT Date_Claim_Made ,  Date_Claim_Settled FROM Claims WHERE Amount_Claimed  >  ( SELECT avg(Amount_Claimed) FROM Claims )
SELECT Date_Claim_Made ,  Date_Claim_Settled FROM Settlements
SELECT Date_Claim_Made FROM Claims ORDER BY Date_Claim_Made ASC LIMIT 1
SELECT Date_Claim_Made FROM Claims WHERE Amount_Settled  <=  ( SELECT avg(Amount_Settled) FROM Claims )
SELECT Date_of_ceremony ,  RESULT FROM music_festival
SELECT Date_of_ceremony FROM music_festival WHERE Category  =  \"Best Song\" AND RESULT  =  \"Awarded\"
SELECT Delegate ,  Committee FROM election
SELECT Delegate FROM election WHERE District  =  1
SELECT Denomination ,  COUNT(*) FROM school GROUP BY Denomination
SELECT Denomination ,  COUNT(*) FROM school GROUP BY Denomination ORDER BY COUNT(*) DESC
SELECT Denomination FROM school GROUP BY Denomination HAVING COUNT(*)  >  1
SELECT Denomination FROM school WHERE Founded  <  1890 INTERSECT SELECT Denomination FROM school WHERE Founded  >  1900
SELECT Dependent_name FROM dependent WHERE relationship  =  'Spouse'
SELECT Description FROM aircraft
SELECT Destroyed_by_Employee_ID ,  count(*) FROM Documents_to_be_destroyed GROUP BY Destroyed_by_Employee_ID
SELECT Destroyed_by_Employee_ID FROM Documents_to_be_destroyed UNION SELECT Destruction_Authorised_by_Employee_ID FROM Documents_to_be_destroyed
SELECT Destruction_Authorised_by_Employee_ID ,  count(*) FROM Documents_to_be_destroyed GROUP BY Destruction_Authorised_by_Employee_ID
SELECT District_name ,  City_Population FROM district WHERE City_Population BETWEEN 200000 AND 2000000
SELECT Draft_Pick_Number ,  Draft_Class FROM match_season WHERE POSITION  =  \"Defender\"
SELECT Duration FROM actor ORDER BY Age DESC LIMIT 1
SELECT Earnings FROM poker_player ORDER BY Earnings DESC
SELECT Election_Cycle ,  count(*) FROM VOTING_RECORD GROUP BY Election_Cycle
SELECT Elimination_Move FROM Elimination WHERE Team  =  \"Team Orton\"
SELECT Enrollment FROM school WHERE Denomination != \"Catholic\"
SELECT Episode FROM TV_series ORDER BY rating
SELECT Event_ID FROM Participants_in_Events GROUP BY Event_ID ORDER BY count(*) DESC LIMIT 1
SELECT FacID FROM Faculty EXCEPT SELECT FacID FROM Faculty_participates_in
SELECT FacID FROM Faculty EXCEPT SELECT advisor FROM Student
SELECT FacID FROM Faculty WHERE Sex  =  'M'
SELECT FacID FROM Faculty_participates_in INTERSECT SELECT advisor FROM Student
SELECT Famous_Release_date FROM artist ORDER BY Age DESC LIMIT 1
SELECT Famous_Title ,  Age FROM artist ORDER BY Age DESC
SELECT Famous_Title FROM artist WHERE Artist  =  \"Triumfall\"
SELECT Famous_Title FROM artist WHERE Artist_ID NOT IN(SELECT Artist_ID FROM volume)
SELECT Festival_Name ,  LOCATION FROM festival_detail
SELECT Festival_Name FROM festival_detail ORDER BY YEAR DESC LIMIT 3
SELECT Festival_Name FROM festival_detail WHERE YEAR  =  2007
SELECT Final_Table_Made ,  Best_Finish FROM poker_player
SELECT FirstName ,  LastName FROM CUSTOMER WHERE Email  =  \"luisg@embraer.com.br\"
SELECT FirstName ,  LastName FROM EMPLOYEE WHERE City  =  \"Calgary\"
SELECT First_year ,  Last_year FROM party WHERE Party_Theme  =  \"Spring\" OR Party_Theme  =  \"Teqnology\"
SELECT FlightNo FROM FLIGHTS WHERE DestAirport  =  \"APG\"
SELECT FlightNo FROM FLIGHTS WHERE SourceAirport  =  \"APG\"
SELECT Fname ,  Lname ,  Age FROM Student WHERE Sex  =  'F'
SELECT Fname ,  Lname ,  phone FROM Faculty WHERE Sex  =  'F'
SELECT Fname ,  Lname FROM FACULTY WHERE sex  =  \"M\" AND Building  =  \"NEB\"
SELECT Fname ,  Lname FROM Student
SELECT Fname FROM FACULTY WHERE Rank  =  \"Professor\" ORDER BY Fname
SELECT Fname FROM STUDENT ORDER BY Age DESC
SELECT Fname FROM STUDENT WHERE Age  >  22
SELECT Fname FROM STUDENT WHERE StuID NOT IN (SELECT StuID FROM ENROLLED_IN)
SELECT Fname FROM Student WHERE Major  =  600
SELECT Gender ,  Occupation FROM player
SELECT Gname FROM Plays_games AS T1 JOIN Video_games AS T2 ON T1.gameid  =  T2.gameid JOIN Student AS T3 ON T3.Stuid  =  T1.Stuid WHERE T3.Lname  =  \"Smith\" AND T3.Fname  =  \"Linda\"
SELECT Governor FROM party GROUP BY Governor ORDER BY COUNT(*) DESC LIMIT 1
SELECT Grape ,  Appelation ,  Name FROM WINE WHERE Score  >  93 ORDER BY Name
SELECT Grape ,  Appelation FROM WINE
SELECT Grape ,  Winery ,  YEAR FROM WINE WHERE Price  >   100 ORDER BY YEAR
SELECT HOST FROM performance ORDER BY Attendance ASC
SELECT Headquarters ,  COUNT(*) FROM company GROUP BY Headquarters
SELECT Headquarters ,  Industry FROM company
SELECT Headquarters FROM Companies GROUP BY Headquarters HAVING COUNT(*)  >  2
SELECT Headquarters FROM company GROUP BY Headquarters HAVING COUNT(*)  >=  2
SELECT Headquarters FROM company GROUP BY Headquarters ORDER BY COUNT(*) DESC LIMIT 1
SELECT Headquarters FROM company ORDER BY Sales_in_Billion DESC LIMIT 1
SELECT Headquarters FROM company WHERE Industry  =  \"Banking\" INTERSECT SELECT Headquarters FROM company WHERE Industry  =  \"Oil and gas\"
SELECT Height ,  Weight FROM people ORDER BY Height DESC
SELECT Hispanic FROM city WHERE Black  >  10
SELECT Hometown ,  COUNT(*) FROM teacher GROUP BY Hometown
SELECT Hometown FROM people WHERE Age  >  23 INTERSECT SELECT Hometown FROM people WHERE Age  <  20
SELECT Hometown FROM teacher GROUP BY Hometown HAVING COUNT(*)  >=  2
SELECT Hometown FROM teacher GROUP BY Hometown ORDER BY COUNT(*) DESC LIMIT 1
SELECT Hometown FROM teacher ORDER BY Age ASC LIMIT 1
SELECT Hosts FROM farm_competition WHERE Theme !=  'Aliens'
SELECT How_to_Get_There ,  COUNT(*) FROM Tourist_Attractions GROUP BY How_to_Get_There
SELECT How_to_Get_There FROM Tourist_Attractions GROUP BY How_to_Get_There ORDER BY COUNT(*) DESC LIMIT 1
SELECT ID FROM Highschooler WHERE name  =  \"Kyle\"
SELECT Industry ,  COUNT(*) FROM Companies GROUP BY Industry
SELECT Industry FROM Companies GROUP BY Industry ORDER BY COUNT(*) DESC
SELECT Industry FROM Companies GROUP BY Industry ORDER BY COUNT(*) DESC LIMIT 1
SELECT Industry FROM Companies WHERE Headquarters  =  \"USA\" INTERSECT SELECT Industry FROM Companies WHERE Headquarters  =  \"China\"
SELECT Injured FROM perpetrator ORDER BY Injured ASC
SELECT International_Passengers ,  Domestic_Passengers FROM airport WHERE Airport_Name  =  \"London Heathrow\"
SELECT Investor ,  COUNT(*) FROM entrepreneur GROUP BY Investor
SELECT Investor FROM entrepreneur GROUP BY Investor HAVING COUNT(*)  >=  2
SELECT Investor FROM entrepreneur GROUP BY Investor ORDER BY COUNT(*) DESC LIMIT 1
SELECT Investor FROM entrepreneur WHERE Money_Requested  >  140000 INTERSECT SELECT Investor FROM entrepreneur WHERE Money_Requested  <  120000
SELECT Investor_details FROM INVESTORS
SELECT Issue_Date FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T1.age  <=  23
SELECT Issue_Date FROM volume ORDER BY Weeks_on_Top ASC LIMIT 1
SELECT Join_Year FROM pilot ORDER BY Rank ASC LIMIT 1
SELECT LANGUAGE ,  CountryCode ,  max(Percentage) FROM countrylanguage GROUP BY CountryCode
SELECT LANGUAGE ,  count(*) FROM TV_Channel GROUP BY LANGUAGE
SELECT LANGUAGE FROM countrylanguage GROUP BY LANGUAGE ORDER BY count(*) DESC LIMIT 1
SELECT LENGTH ,  Height FROM roller_coaster
SELECT LEVEL FROM member GROUP BY LEVEL ORDER BY count(*) DESC LIMIT 1
SELECT LName FROM STUDENT WHERE Major  =  50
SELECT LName FROM STUDENT WHERE Sex  =  \"F\" ORDER BY Age DESC
SELECT LName FROM Student WHERE age  =  (SELECT min(age) FROM Student)
SELECT LOCATION ,  COUNT(*) FROM performance GROUP BY LOCATION
SELECT LOCATION ,  COUNT(*) FROM railway GROUP BY LOCATION
SELECT LOCATION ,  count(*) FROM cinema GROUP BY LOCATION
SELECT LOCATION ,  count(*) FROM gas_station GROUP BY LOCATION ORDER BY count(*)
SELECT LOCATION ,  name FROM stadium WHERE capacity BETWEEN 5000 AND 10000
SELECT LOCATION ,  sum(number_of_platforms) ,  sum(total_passengers) FROM station GROUP BY LOCATION
SELECT LOCATION FROM bridge WHERE name  =  'Kolob Arch' OR name  =  'Rainbow Bridge'
SELECT LOCATION FROM cinema EXCEPT SELECT LOCATION FROM cinema WHERE capacity  >  800
SELECT LOCATION FROM cinema WHERE capacity  >  300 GROUP BY LOCATION HAVING count(*)  >  1
SELECT LOCATION FROM cinema WHERE capacity  >  300 GROUP BY LOCATION HAVING count(*)  >=  2
SELECT LOCATION FROM cinema WHERE openning_year  =  2010 INTERSECT SELECT LOCATION FROM cinema WHERE openning_year  =  2011
SELECT LOCATION FROM cinema WHERE openning_year  >=  2010 GROUP BY LOCATION ORDER BY count(*) DESC LIMIT 1
SELECT LOCATION FROM county_public_safety GROUP BY LOCATION ORDER BY COUNT(*) DESC LIMIT 1
SELECT LOCATION FROM festival_detail ORDER BY Num_of_Audience DESC LIMIT 1
SELECT LOCATION FROM gas_station ORDER BY open_year
SELECT LOCATION FROM party ORDER BY Number_of_hosts DESC LIMIT 1
SELECT LOCATION FROM performance GROUP BY LOCATION HAVING COUNT(*)  >=  2
SELECT LOCATION FROM performance GROUP BY LOCATION ORDER BY COUNT(*) DESC LIMIT 1
SELECT LOCATION FROM performance WHERE Attendance  >  2000 INTERSECT SELECT LOCATION FROM performance WHERE Attendance  <  1000
SELECT LOCATION FROM perpetrator ORDER BY Killed DESC LIMIT 1
SELECT LOCATION FROM railway GROUP BY LOCATION HAVING COUNT(*)  >  1
SELECT LOCATION FROM school ORDER BY Enrollment ASC
SELECT LOCATION FROM school ORDER BY Founded DESC
SELECT LOCATION FROM school WHERE School_ID NOT IN (SELECT School_ID FROM Player)
SELECT LOCATION FROM shop WHERE Open_Year  >  2012 INTERSECT SELECT LOCATION FROM shop WHERE Open_Year  <  2008
SELECT LOCATION FROM station EXCEPT SELECT LOCATION FROM station WHERE number_of_platforms  >=  15
SELECT LOCATION FROM station GROUP BY LOCATION HAVING count(*)  =  1
SELECT LOCATION FROM station GROUP BY LOCATION ORDER BY count(*) DESC LIMIT 1
SELECT LOCATION FROM track WHERE seating  >  90000 INTERSECT SELECT LOCATION FROM track WHERE seating  <  70000
SELECT LOCATION FROM wrestler GROUP BY LOCATION HAVING COUNT(*)  >  2
SELECT LastName FROM CUSTOMER EXCEPT SELECT T1.LastName FROM CUSTOMER AS T1 JOIN Invoice AS T2 ON T1.CustomerId  =  T2.CustomerId WHERE T2.total  >  20
SELECT Leader_Name ,  College_Location FROM college
SELECT Lieutenant_Governor ,  Comptroller FROM party WHERE Party  =  \"Democratic\"
SELECT Lifespan FROM representative WHERE State  =  \"New York\" OR State  =  \"Indiana\"
SELECT Lname FROM FACULTY WHERE Building  =  \"Barton\" ORDER BY Lname
SELECT Location_Name FROM LOCATIONS WHERE Location_Name LIKE \"%film%\"
SELECT Low_Estimate ,  High_Estimate FROM film_market_estimation
SELECT MAX(cost) ,  MIN(cost) ,  AVG(cost) FROM procedures
SELECT MIN(salary) ,  department_id FROM employees GROUP BY department_id
SELECT MONTH FROM happy_hour GROUP BY MONTH HAVING count(*)  >  2
SELECT MONTH FROM happy_hour GROUP BY MONTH ORDER BY count(*) DESC LIMIT 1
SELECT Major FROM STUDENT GROUP BY Major HAVING COUNT(*)  <  3
SELECT Major FROM STUDENT GROUP BY major ORDER BY count(*) DESC LIMIT 1
SELECT Major FROM STUDENT WHERE Sex  =  \"F\" GROUP BY major ORDER BY count(*) DESC LIMIT 1
SELECT Major FROM STUDENT WHERE Sex  =  \"M\"
SELECT Major_Record_Format FROM orchestra GROUP BY Major_Record_Format ORDER BY COUNT(*) ASC
SELECT Market_Rate ,  name FROM furniture WHERE Furniture_ID NOT IN (SELECT Furniture_ID FROM furniture_manufacte)
SELECT Marketing_Region_Code FROM Drama_Workshop_Groups GROUP BY Marketing_Region_Code ORDER BY count(*) DESC LIMIT 1
SELECT Marketing_Region_Descriptrion FROM Marketing_Regions WHERE Marketing_Region_Name  =  \"China\"
SELECT Membership_card FROM member GROUP BY Membership_card HAVING count(*)  >  5
SELECT Memory_in_G ,  Carrier FROM phone
SELECT Money_Rank FROM poker_player ORDER BY Earnings DESC LIMIT 1
SELECT Name ,  Age FROM editor
SELECT Name ,  Arrival FROM train
SELECT Name ,  Birth_Date FROM people ORDER BY Name ASC
SELECT Name ,  Country FROM member
SELECT Name ,  LOCATION FROM wrestler
SELECT Name ,  Nationality FROM HOST ORDER BY Age DESC LIMIT 1
SELECT Name ,  Opening_Hours FROM TOURIST_ATTRACTIONS WHERE How_to_Get_There  =  \"bus\" OR How_to_Get_There  =  \"walk\"
SELECT Name ,  Other_Details FROM Staff
SELECT Name ,  Party FROM representative
SELECT Name ,  Population ,  LifeExpectancy FROM country WHERE Continent  =  \"Asia\" ORDER BY SurfaceArea DESC LIMIT 1
SELECT Name ,  Score FROM WINE
SELECT Name ,  SurfaceArea ,  IndepYear FROM country ORDER BY Population LIMIT 1
SELECT Name ,  SurfaceArea FROM country ORDER BY SurfaceArea DESC LIMIT 5
SELECT Name ,  birth_place FROM people EXCEPT SELECT T1.Name ,  T1.birth_place FROM people AS T1 JOIN body_builder AS T2 ON T1.people_id  =  T2.people_id
SELECT Name ,  population ,  HeadOfState FROM country ORDER BY SurfaceArea DESC LIMIT 1
SELECT Name FROM  people WHERE Age  =  35 OR Age  =  36
SELECT Name FROM ARTIST EXCEPT SELECT T2.Name FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistId
SELECT Name FROM ARTIST WHERE Name LIKE \"%a%\"
SELECT Name FROM Artwork WHERE Artwork_ID NOT IN (SELECT Artwork_ID FROM nomination)
SELECT Name FROM GENRE
SELECT Name FROM HOST WHERE Host_ID NOT IN (SELECT Host_ID FROM party_host)
SELECT Name FROM People ORDER BY Age DESC LIMIT 5
SELECT Name FROM People ORDER BY Height ASC
SELECT Name FROM People ORDER BY Name ASC
SELECT Name FROM People ORDER BY Weight ASC
SELECT Name FROM Products
SELECT Name FROM Projects WHERE Code NOT IN (SELECT Project FROM AssignedTo)
SELECT Name FROM Representative WHERE Party != \"Republican\"
SELECT Name FROM TOURIST_ATTRACTIONS WHERE How_to_Get_There  =  \"bus\"
SELECT Name FROM TRACK WHERE Name LIKE '%you%'
SELECT Name FROM Team
SELECT Name FROM WINE ORDER BY Score LIMIT 1
SELECT Name FROM WINE WHERE Price  >  (SELECT max(Price) FROM WINE WHERE YEAR  =  2006)
SELECT Name FROM WINE WHERE YEAR  <  (SELECT min(YEAR) FROM WINE WHERE Winery  =  \"Brander\")
SELECT Name FROM WINE WHERE YEAR  =  \"2008\"
SELECT Name FROM WINE WHERE score  >  90
SELECT Name FROM actor ORDER BY Name ASC
SELECT Name FROM actor WHERE Age != 20
SELECT Name FROM artwork ORDER BY Name ASC
SELECT Name FROM artwork WHERE TYPE != \"Program Talent Show\"
SELECT Name FROM city ORDER BY Name ASC
SELECT Name FROM city ORDER BY White DESC LIMIT 5
SELECT Name FROM climber ORDER BY Points DESC
SELECT Name FROM climber WHERE Country != \"Switzerland\"
SELECT Name FROM company ORDER BY Sales_in_Billion ASC
SELECT Name FROM company WHERE Industry  =  \"Banking\" OR Industry  =  \"Retailing\"
SELECT Name FROM conductor ORDER BY Age ASC
SELECT Name FROM conductor ORDER BY Year_of_Work DESC
SELECT Name FROM conductor ORDER BY Year_of_Work DESC LIMIT 1
SELECT Name FROM conductor WHERE Nationality != 'USA'
SELECT Name FROM country ORDER BY Population ASC LIMIT 3
SELECT Name FROM country ORDER BY Population DESC LIMIT 3
SELECT Name FROM country WHERE Continent  =  \"Africa\"  AND population  <  (SELECT max(population) FROM country WHERE Continent  =  \"Asia\")
SELECT Name FROM country WHERE Continent  =  \"Africa\"  AND population  <  (SELECT min(population) FROM country WHERE Continent  =  \"Asia\")
SELECT Name FROM country WHERE Continent  =  \"Asia\"  AND population  >  (SELECT max(population) FROM country WHERE Continent  =  \"Africa\")
SELECT Name FROM country WHERE Continent  =  \"Asia\"  AND population  >  (SELECT min(population) FROM country WHERE Continent  =  \"Africa\")
SELECT Name FROM country WHERE Continent  =  \"Asia\" ORDER BY LifeExpectancy LIMIT 1
SELECT Name FROM country WHERE IndepYear  >  1950
SELECT Name FROM country WHERE Languages != \"German\"
SELECT Name FROM country WHERE SurfaceArea  >  (SELECT min(SurfaceArea) FROM country WHERE Continent  =  \"Europe\")
SELECT Name FROM country WHERE continent  =  \"Europe\" AND Population  =  \"80000\"
SELECT Name FROM county_public_safety ORDER BY Population DESC
SELECT Name FROM county_public_safety ORDER BY Population DESC LIMIT 1
SELECT Name FROM county_public_safety WHERE County_ID NOT IN (SELECT County_ID FROM city)
SELECT Name FROM editor ORDER BY Age ASC
SELECT Name FROM editor ORDER BY Age ASC LIMIT 1
SELECT Name FROM editor WHERE Age  =  24 OR Age  =  25
SELECT Name FROM editor WHERE Age  >  25
SELECT Name FROM editor WHERE editor_id NOT IN (SELECT editor_id FROM journal_committee)
SELECT Name FROM journalist ORDER BY Years_working ASC
SELECT Name FROM journalist WHERE Nationality  =  \"England\" OR Nationality  =  \"Wales\"
SELECT Name FROM journalist WHERE journalist_ID NOT IN (SELECT journalist_ID FROM news_report)
SELECT Name FROM manager ORDER BY Age DESC LIMIT 1
SELECT Name FROM manager ORDER BY LEVEL ASC
SELECT Name FROM member ORDER BY Name ASC
SELECT Name FROM member WHERE Country  =  \"United States\" OR Country  =  \"Canada\"
SELECT Name FROM member WHERE Member_ID NOT IN (SELECT Member_ID FROM member_attendance)
SELECT Name FROM member WHERE Member_ID NOT IN (SELECT Member_ID FROM round)
SELECT Name FROM mountain ORDER BY Height DESC LIMIT 1
SELECT Name FROM mountain ORDER BY Name ASC
SELECT Name FROM mountain WHERE Height  >  5000 OR Prominence  >  1000
SELECT Name FROM mountain WHERE Mountain_ID NOT IN (SELECT Mountain_ID FROM climber)
SELECT Name FROM musical WHERE Musical_ID NOT IN (SELECT Musical_ID FROM actor)
SELECT Name FROM musical WHERE Nominee  =  \"Bob Fosse\"
SELECT Name FROM people WHERE Nationality != \"Russia\"
SELECT Name FROM people WHERE People_ID NOT IN (SELECT People_ID FROM employment)
SELECT Name FROM people WHERE People_ID NOT IN (SELECT People_ID FROM entrepreneur)
SELECT Name FROM people WHERE People_ID NOT IN (SELECT People_ID FROM perpetrator)
SELECT Name FROM people WHERE People_ID NOT IN (SELECT People_ID FROM poker_player)
SELECT Name FROM people WHERE People_id NOT IN (SELECT Affirmative FROM debate_people)
SELECT Name FROM phone ORDER BY Price ASC
SELECT Name FROM phone WHERE Carrier  =  \"Sprint\" OR Carrier  =  \"TMobile\"
SELECT Name FROM phone WHERE Phone_id NOT IN (SELECT Phone_ID FROM phone_market)
SELECT Name FROM pilot ORDER BY Age DESC
SELECT Name FROM pilot ORDER BY Name ASC
SELECT Name FROM pilot WHERE Age  <=  30 ORDER BY Name DESC
SELECT Name FROM pilot WHERE Age  >=  25
SELECT Name FROM representative WHERE Representative_ID NOT IN (SELECT Representative_ID FROM election)
SELECT Name FROM roller_coaster ORDER BY LENGTH ASC
SELECT Name FROM scientists WHERE ssn NOT IN (SELECT scientist FROM AssignedTo)
SELECT Name FROM ship ORDER BY Tonnage ASC
SELECT Name FROM ship ORDER BY Tonnage DESC LIMIT 1
SELECT Name FROM ship WHERE Nationality  =  \"United States\" OR Nationality  =  \"United Kingdom\"
SELECT Name FROM ship WHERE Nationality != \"United States\"
SELECT Name FROM ship WHERE Ship_ID NOT IN (SELECT Ship_ID FROM mission)
SELECT Name FROM singer ORDER BY Net_Worth_Millions ASC
SELECT Name FROM singer ORDER BY Net_Worth_Millions DESC LIMIT 1
SELECT Name FROM singer WHERE Birth_Year  =  1948 OR Birth_Year  =  1949
SELECT Name FROM singer WHERE Citizenship != \"France\"
SELECT Name FROM singer WHERE Singer_ID NOT IN (SELECT Singer_ID FROM song)
SELECT Name FROM teacher ORDER BY Age ASC
SELECT Name FROM teacher WHERE Age  =  32 OR Age  =  33
SELECT Name FROM teacher WHERE Teacher_id NOT IN (SELECT Teacher_id FROM course_arrange)
SELECT Name FROM team WHERE Team_id NOT IN (SELECT Team FROM match_season)
SELECT Name FROM technician ORDER BY Age ASC
SELECT Name FROM technician WHERE Age  =  36 OR Age  =  37
SELECT Name FROM technician WHERE Team != \"NYY\"
SELECT Name FROM technician WHERE technician_id NOT IN (SELECT technician_id FROM repair_assignment)
SELECT Name FROM wrestler ORDER BY Days_held ASC LIMIT 1
SELECT Name FROM wrestler ORDER BY Days_held DESC
SELECT Name FROM wrestler WHERE Days_held  <  100
SELECT Name FROM wrestler WHERE Wrestler_ID NOT IN (SELECT Wrestler_ID FROM elimination)
SELECT Nationality ,  Age FROM journalist
SELECT Nationality ,  COUNT(*) FROM HOST GROUP BY Nationality
SELECT Nationality ,  COUNT(*) FROM journalist GROUP BY Nationality
SELECT Nationality ,  COUNT(*) FROM people GROUP BY Nationality
SELECT Nationality ,  COUNT(*) FROM pilot GROUP BY Nationality
SELECT Nationality FROM HOST GROUP BY Nationality ORDER BY COUNT(*) DESC LIMIT 1
SELECT Nationality FROM HOST WHERE Age  >  45 INTERSECT SELECT Nationality FROM HOST WHERE Age  <  35
SELECT Nationality FROM journalist GROUP BY Nationality ORDER BY COUNT(*) DESC LIMIT 1
SELECT Nationality FROM journalist ORDER BY Years_working DESC LIMIT 1
SELECT Nationality FROM journalist WHERE Years_working  >  10 INTERSECT SELECT Nationality FROM journalist WHERE Years_working  <  3
SELECT Nationality FROM people GROUP BY Nationality HAVING COUNT(*)  >=  2
SELECT Nationality FROM people GROUP BY Nationality ORDER BY COUNT(*) DESC LIMIT 1
SELECT Nationality FROM pilot GROUP BY Nationality ORDER BY COUNT(*) DESC LIMIT 1
SELECT Nationality FROM ship GROUP BY Nationality HAVING COUNT(*)  >  2
SELECT Nickname FROM school_details WHERE Division != \"Division 1\"
SELECT Nominee ,  COUNT(*) FROM musical GROUP BY Nominee
SELECT Nominee FROM musical GROUP BY Nominee HAVING COUNT(*)  >  2
SELECT Nominee FROM musical GROUP BY Nominee ORDER BY COUNT(*) DESC LIMIT 1
SELECT Nominee FROM musical WHERE Award  =  \"Tony Award\" INTERSECT SELECT Nominee FROM musical WHERE Award  =  \"Drama Desk Award\"
SELECT Nominee FROM musical WHERE Award  =  \"Tony Award\" OR Award  =  \"Cleavant Derricks\"
SELECT Num_of_Audience FROM festival_detail WHERE YEAR  =  2008 OR YEAR  =  2010
SELECT Num_of_Component ,  name FROM furniture WHERE Num_of_Component  >  10
SELECT Num_of_Staff ,  Open_Year FROM museum WHERE name  =  'Plaza Museum'
SELECT OWNER FROM channel ORDER BY rating_in_percent DESC LIMIT 1
SELECT ObjectNumber FROM railway WHERE Railway_ID NOT IN (SELECT Railway_ID FROM train)
SELECT Occupation ,  COUNT(*) FROM player GROUP BY Occupation
SELECT Occupation FROM player GROUP BY Occupation ORDER BY COUNT(*) DESC LIMIT 1
SELECT Official_Name ,  Status FROM city ORDER BY Population DESC LIMIT 1
SELECT Official_Name FROM city ORDER BY Population DESC
SELECT Official_Name FROM city WHERE City_ID NOT IN (SELECT Host_city_ID FROM farm_competition)
SELECT Official_Name FROM city WHERE Population  >  1500 OR Population  <  500
SELECT Official_native_language FROM country WHERE Official_native_language LIKE \"%English%\"
SELECT Open_Date ,  Open_Year FROM shop WHERE Shop_Name  =  \"Apple\"
SELECT Operating_system ,  count(*) FROM web_client_accelerator GROUP BY Operating_system
SELECT Orchestra FROM orchestra WHERE Orchestra_ID NOT IN (SELECT Orchestra_ID FROM performance)
SELECT Order_Date FROM BOOKINGS
SELECT Other_Details FROM LOCATIONS WHERE Location_Name  =  \"UK Gallery\"
SELECT POSITION ,  COUNT(*) FROM match_season GROUP BY POSITION
SELECT POSITION ,  Team FROM pilot
SELECT POSITION ,  avg(Points) FROM player GROUP BY POSITION
SELECT POSITION FROM match_season GROUP BY POSITION ORDER BY count(*) DESC LIMIT 1
SELECT POSITION FROM pilot WHERE Join_Year       <  2000 INTERSECT SELECT POSITION FROM pilot WHERE Join_Year    >  2005
SELECT POSITION FROM player GROUP BY name HAVING avg(Points)  >=  20
SELECT POSITION FROM player WHERE Points  >  20 INTERSECT SELECT POSITION FROM player WHERE Points  <  10
SELECT Park FROM roller_coaster ORDER BY Speed DESC LIMIT 1
SELECT Participant_ID ,  Participant_Type_Code ,  Participant_Details FROM  Participants
SELECT Party ,  COUNT(*) FROM people GROUP BY Party
SELECT Party ,  COUNT(*) FROM representative GROUP BY Party
SELECT Party ,  COUNT(*) FROM representative GROUP BY Party ORDER BY COUNT(*) DESC LIMIT 1
SELECT Party FROM party GROUP BY Party HAVING COUNT(*)  >=  2
SELECT Party FROM party WHERE Party_ID NOT IN (SELECT Party FROM election)
SELECT Party FROM people GROUP BY Party ORDER BY COUNT(*) DESC LIMIT 1
SELECT Party FROM people ORDER BY Age ASC LIMIT 1
SELECT Party FROM representative GROUP BY Party HAVING COUNT(*)  >=  3
SELECT Party FROM representative WHERE State  =  \"New York\" INTERSECT SELECT Party FROM representative WHERE State  =  \"Pennsylvania\"
SELECT Party_Theme ,  LOCATION FROM party
SELECT Party_Theme FROM party ORDER BY Number_of_hosts ASC
SELECT Payment_ID ,  Date_Payment_Made ,  Amount_Payment FROM Payments WHERE Payment_Method_Code  =  'Visa'
SELECT Payment_Method_Code ,  Date_Payment_Made ,  Amount_Payment FROM Payments ORDER BY Date_Payment_Made ASC
SELECT Payment_Method_Code FROM Payments GROUP BY Payment_Method_Code ORDER BY count(*) ASC LIMIT 1
SELECT Payment_Method_Code FROM Payments GROUP BY Payment_Method_Code ORDER BY count(*) DESC LIMIT 1
SELECT Phone FROM EMPLOYEE
SELECT Pilot_name FROM pilot ORDER BY Rank ASC
SELECT Pilot_name FROM pilot WHERE Pilot_ID NOT IN (SELECT Pilot_ID FROM pilot_record)
SELECT Pilot_name FROM pilot WHERE Team  =  \"Bradley\" OR Team  =  \"Fordham\"
SELECT Pixel_aspect_ratio_PAR ,  country FROM tv_channel WHERE LANGUAGE != 'English'
SELECT Planned_Delivery_Date ,  Actual_Delivery_Date FROM BOOKINGS
SELECT Player ,  Years_Played FROM player
SELECT Player FROM match_season WHERE College  =  \"UCLA\"
SELECT Player_name ,  residence FROM player WHERE Occupation != \"Researcher\"
SELECT Player_name FROM player ORDER BY Votes ASC
SELECT Player_name FROM player ORDER BY Votes DESC LIMIT 1
SELECT Player_name FROM player WHERE Player_ID NOT IN (SELECT Player_ID FROM player_coach)
SELECT Police_force ,  COUNT(*) FROM county_public_safety GROUP BY Police_force
SELECT Police_force FROM county_public_safety WHERE LOCATION  =  \"East\" INTERSECT SELECT Police_force FROM county_public_safety WHERE LOCATION  =  \"West\"
SELECT Policy_Type_Code FROM Customer_Policies GROUP BY Policy_Type_Code ORDER BY count(*) DESC LIMIT 1
SELECT Population ,  LifeExpectancy FROM country WHERE Name  =  \"Brazil\"
SELECT Population ,  Region FROM country WHERE Name  =  \"Angola\"
SELECT Price FROM WINE WHERE YEAR  <  2010
SELECT Price FROM publication WHERE Publisher  =  \"Person\" OR Publisher  =  \"Wiley\"
SELECT Product_ID FROM INVOICES GROUP BY Product_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT Product_Name ,  Product_Type_Code FROM Products
SELECT Product_Name ,  avg(Product_Price) FROM PRODUCTS GROUP BY Product_Name
SELECT Product_Name FROM PRODUCTS GROUP BY Product_Name HAVING avg(Product_Price)  <  1000000
SELECT Product_Name FROM PRODUCTS ORDER BY Product_Price DESC LIMIT 1
SELECT Product_Name FROM Products ORDER BY Product_Price ASC
SELECT Product_Name FROM Products ORDER BY Product_Price DESC LIMIT 1
SELECT Product_Name FROM Products WHERE Product_ID NOT IN (SELECT Product_ID FROM Products_in_Events)
SELECT Product_Price FROM Products WHERE Product_Name  =  \"Dining\" OR Product_Name  =  \"Trading Policy\"
SELECT Product_Type_Code ,  COUNT(*) FROM Products GROUP BY Product_Type_Code
SELECT Product_Type_Code FROM Products GROUP BY Product_Type_Code HAVING COUNT(*)  >=  2
SELECT Product_Type_Code FROM Products GROUP BY Product_Type_Code ORDER BY COUNT(*) DESC LIMIT 1
SELECT Product_Type_Code FROM Products WHERE Product_Price  >  4500 INTERSECT SELECT Product_Type_Code FROM Products WHERE Product_Price  <  3000
SELECT Publication_Date FROM publication GROUP BY Publication_Date ORDER BY COUNT(*) DESC LIMIT 1
SELECT Publication_Date FROM publication ORDER BY Price ASC LIMIT 3
SELECT Publication_Date FROM publication ORDER BY Price DESC
SELECT Publisher ,  COUNT(*) FROM publication GROUP BY Publisher
SELECT Publisher FROM publication GROUP BY Publisher HAVING COUNT(*)  >  1
SELECT Publisher FROM publication ORDER BY Price DESC LIMIT 1
SELECT Publisher FROM publication WHERE Price  >  10000000 INTERSECT SELECT Publisher FROM publication WHERE Price  <  5000000
SELECT RESULT ,  COUNT(*) FROM music_festival GROUP BY RESULT ORDER BY COUNT(*) DESC
SELECT RESULT FROM music_festival GROUP BY RESULT ORDER BY COUNT(*) DESC LIMIT 1
SELECT RESULT FROM musical GROUP BY RESULT ORDER BY COUNT(*) DESC LIMIT 1
SELECT Range FROM mountain GROUP BY Range ORDER BY COUNT(*) DESC LIMIT 1
SELECT Rank FROM FACULTY GROUP BY Rank ORDER BY count(*) ASC LIMIT 1
SELECT Record_Company ,  COUNT(*) FROM orchestra GROUP BY Record_Company
SELECT Record_Company FROM orchestra GROUP BY Record_Company ORDER BY COUNT(*) DESC LIMIT 1
SELECT Record_Company FROM orchestra ORDER BY Year_of_Founded DESC
SELECT Record_Company FROM orchestra WHERE Year_of_Founded  <  2003 INTERSECT SELECT Record_Company FROM orchestra WHERE Year_of_Founded  >  2003
SELECT Region FROM country AS T1 JOIN city AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Name  =  \"Kabul\"
SELECT Reign ,  Days_held FROM wrestler
SELECT Reign FROM wrestler GROUP BY Reign ORDER BY COUNT(*) DESC LIMIT 1
SELECT Residence FROM player GROUP BY Residence HAVING COUNT(*)  >=  2
SELECT Residence FROM player WHERE gender  =  \"M\" INTERSECT SELECT Residence FROM player WHERE gender  =  \"F\"
SELECT Room FROM FACULTY WHERE Rank  =  \"Professor\" AND Building  =  \"NEB\"
SELECT STATEMENT_ID ,  count(*) FROM Accounts GROUP BY STATEMENT_ID
SELECT STATEMENT_ID ,  statement_details FROM Statements
SELECT School_Colors FROM school ORDER BY Enrollment DESC LIMIT 1
SELECT Shop_Name ,  LOCATION FROM shop ORDER BY Shop_Name ASC
SELECT Shop_Name FROM shop ORDER BY Open_Year ASC
SELECT Shop_Name FROM shop ORDER BY Open_Year DESC LIMIT 1
SELECT Shop_Name FROM shop WHERE Shop_ID NOT IN (SELECT Shop_ID FROM stock)
SELECT Snatch ,  Clean_Jerk FROM body_builder ORDER BY Snatch ASC
SELECT Software_Platform ,  COUNT(*) FROM device GROUP BY Software_Platform
SELECT Software_Platform FROM device GROUP BY Software_Platform ORDER BY COUNT(*) DESC
SELECT Software_Platform FROM device GROUP BY Software_Platform ORDER BY COUNT(*) DESC LIMIT 1
SELECT Song FROM volume ORDER BY Song
SELECT Song FROM volume ORDER BY Weeks_on_Top DESC LIMIT 1
SELECT Song FROM volume WHERE Weeks_on_Top  >  1
SELECT Speed FROM roller_coaster ORDER BY LENGTH DESC LIMIT 1
SELECT Sponsor_name FROM player WHERE Residence  =  \"Brandon\" OR Residence  =  \"Birtle\"
SELECT Starting_Year FROM technician ORDER BY Age DESC LIMIT 1
SELECT Starting_Year FROM technician WHERE Team  =  \"CLE\" INTERSECT SELECT Starting_Year FROM technician WHERE Team  =  \"CWS\"
SELECT State FROM representative GROUP BY State HAVING COUNT(*)  >=  2
SELECT Status ,  COUNT(*) FROM roller_coaster GROUP BY Status
SELECT Status ,  avg(Population) FROM city GROUP BY Status
SELECT Status FROM city GROUP BY Status ORDER BY COUNT(*) ASC
SELECT Status FROM city GROUP BY Status ORDER BY COUNT(*) DESC LIMIT 1
SELECT Status FROM city WHERE Population  >  1500 INTERSECT SELECT Status FROM city WHERE Population  <  500
SELECT Status FROM roller_coaster GROUP BY Status HAVING COUNT(*)  >  2
SELECT Status FROM roller_coaster GROUP BY Status ORDER BY COUNT(*) DESC LIMIT 1
SELECT Status FROM roller_coaster WHERE LENGTH  >  3300 OR Height  >  100
SELECT Status_Code FROM BOOKINGS GROUP BY Status_Code ORDER BY count(*) DESC LIMIT 1
SELECT Store_Name FROM Drama_Workshop_Groups
SELECT Stories FROM buildings ORDER BY Height DESC LIMIT 1
SELECT StuID ,  count(*) ,  sum(gamesplayed) FROM Sportsinfo GROUP BY StuID
SELECT StuID FROM Has_allergy GROUP BY StuID HAVING count(*)  >=  2
SELECT StuID FROM Participates_in INTERSECT SELECT StuID FROM Student WHERE age  <  20
SELECT StuID FROM Sportsinfo GROUP BY StuID HAVING sum(hoursperweek)  >  10
SELECT StuID FROM Sportsinfo INTERSECT SELECT StuID FROM Plays_games
SELECT StuID FROM Sportsinfo WHERE onscholarship  =  'Y'
SELECT StuID FROM Student EXCEPT SELECT StuID FROM Has_allergy
SELECT StuID FROM Student EXCEPT SELECT StuID FROM Participates_in
SELECT StuID FROM Student EXCEPT SELECT StuID FROM Plays_games
SELECT StuID FROM Student EXCEPT SELECT StuID FROM Sportsinfo
SELECT StuID FROM Student WHERE Advisor  =  1121
SELECT StuID FROM Student WHERE Sex  =  'M'
SELECT StuID FROM Student WHERE age  =  (SELECT max(age) FROM Student)
SELECT StuID FROM Student WHERE age  >  20
SELECT StuID FROM Student WHERE city_code  =  \"CHI\"
SELECT StuID FROM Student WHERE major  =  600 INTERSECT SELECT StuID FROM Sportsinfo WHERE onscholarship  =  'Y'
SELECT StuID FROM Student WHERE sex  =  'F' INTERSECT SELECT StuID FROM Sportsinfo WHERE sportname  =  \"Football\"
SELECT StuID FROM Student WHERE sex  =  'M' EXCEPT SELECT StuID FROM Sportsinfo WHERE sportname  =  \"Football\"
SELECT Studio ,  COUNT(*) FROM film GROUP BY Studio
SELECT Studio FROM film EXCEPT SELECT Studio FROM film WHERE Director  =  \"Walter Hill\"
SELECT Studio FROM film GROUP BY Studio HAVING COUNT(*)  >=  2
SELECT Studio FROM film GROUP BY Studio HAVING avg(Gross_in_dollar)  >=  4500000
SELECT Studio FROM film GROUP BY Studio ORDER BY COUNT(*) DESC LIMIT 1
SELECT Studio FROM film WHERE Director  =  \"Nicholas Meyer\" INTERSECT SELECT Studio FROM film WHERE Director  =  \"Walter Hill\"
SELECT Stuid ,  sum(hours_played) FROM Plays_games GROUP BY Stuid
SELECT Support_rate ,  Consider_rate ,  Oppose_rate FROM candidate ORDER BY unsure_rate
SELECT T1.Abbreviation ,  T1.Country FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline GROUP BY T1.Airline ORDER BY count(*) LIMIT 1
SELECT T1.Actual_Delivery_Date FROM Customer_Orders AS T1 JOIN ORDER_ITEMS AS T2 ON T1.Order_ID  =  T2.Order_ID WHERE T2.Order_Quantity  =  1
SELECT T1.Aircraft ,  COUNT(*) FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft
SELECT T1.Aircraft ,  T1.Description FROM aircraft AS T1 JOIN airport_aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN airport AS T3 ON T2.Airport_ID  =  T3.Airport_ID WHERE T3.Total_Passengers  >  10000000
SELECT T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft HAVING COUNT(*)  >=  2
SELECT T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft GROUP BY T2.Winning_Aircraft ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Aircraft FROM aircraft AS T1 JOIN airport_aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN airport AS T3 ON T2.Airport_ID  =  T3.Airport_ID WHERE T3.Airport_Name  =  \"London Gatwick\"
SELECT T1.Aircraft FROM aircraft AS T1 JOIN airport_aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN airport AS T3 ON T2.Airport_ID  =  T3.Airport_ID WHERE T3.Airport_Name  =  \"London Heathrow\" INTERSECT SELECT T1.Aircraft FROM aircraft AS T1 JOIN airport_aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN airport AS T3 ON T2.Airport_ID  =  T3.Airport_ID WHERE T3.Airport_Name  =  \"London Gatwick\"
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline GROUP BY T1.Airline HAVING count(*)  <  200
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline GROUP BY T1.Airline HAVING count(*)  >  10
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline GROUP BY T1.Airline ORDER BY count(*) DESC LIMIT 1
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.DestAirport  =  \"AHD\"
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.SourceAirport  =  \"AHD\"
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.SourceAirport  =  \"APG\" INTERSECT SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.SourceAirport  =  \"CVO\"
SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.SourceAirport  =  \"CVO\" EXCEPT SELECT T1.Airline FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T1.uid  =  T2.Airline WHERE T2.SourceAirport  =  \"APG\"
SELECT T1.AirportCode FROM AIRPORTS AS T1 JOIN FLIGHTS AS T2 ON T1.AirportCode  =  T2.DestAirport OR T1.AirportCode  =  T2.SourceAirport GROUP BY T1.AirportCode ORDER BY count(*) DESC LIMIT 1
SELECT T1.AirportCode FROM AIRPORTS AS T1 JOIN FLIGHTS AS T2 ON T1.AirportCode  =  T2.DestAirport OR T1.AirportCode  =  T2.SourceAirport GROUP BY T1.AirportCode ORDER BY count(*) LIMIT 1
SELECT T1.Allergy ,  T1.AllergyType FROM Allergy_type AS T1 JOIN Has_allergy AS T2 ON T1.Allergy  =  T2.Allergy JOIN Student AS T3 ON T3.StuID  =  T2.StuID WHERE T3.Fname  =  \"Lisa\" ORDER BY T1.Allergy
SELECT T1.Area FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation GROUP BY T2.Appelation HAVING T2.year  <  2010 ORDER BY count(*) DESC LIMIT 1
SELECT T1.Attraction_Type_Description ,  T2.Attraction_Type_Code FROM Ref_Attraction_Types AS T1 JOIN Tourist_Attractions AS T2 ON T1.Attraction_Type_Code  =  T2.Attraction_Type_Code GROUP BY T2.Attraction_Type_Code ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Birth_Date FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Earnings ASC LIMIT 1
SELECT T1.Builder FROM railway AS T1 JOIN train AS T2 ON T1.Railway_ID  =  T2.Railway_ID WHERE T2.Name  =  \"Andaman Exp\"
SELECT T1.CName FROM COURSE AS T1 JOIN ENROLLED_IN AS T2 ON T1.CID  =  T2.CID GROUP BY T2.CID HAVING COUNT(*)  >=  5
SELECT T1.City FROM AIRPORTS AS T1 JOIN FLIGHTS AS T2 ON T1.AirportCode  =  T2.DestAirport GROUP BY T1.City ORDER BY count(*) DESC LIMIT 1
SELECT T1.City FROM AIRPORTS AS T1 JOIN FLIGHTS AS T2 ON T1.AirportCode  =  T2.SourceAirport GROUP BY T1.City ORDER BY count(*) DESC LIMIT 1
SELECT T1.City_Town ,  count(*) FROM Addresses AS T1 JOIN Drama_Workshop_Groups AS T2 ON T1.Address_ID  =  T2.Address_ID GROUP BY T1.City_Town
SELECT T1.City_Town FROM Addresses AS T1 JOIN Customers AS T2 ON T1.Address_ID  =  T2.Address_ID EXCEPT SELECT T1.City_Town FROM Addresses AS T1 JOIN Performers AS T2 ON T1.Address_ID  =  T2.Address_ID
SELECT T1.City_Town FROM Addresses AS T1 JOIN Stores AS T2 ON T1.Address_ID  =  T2.Address_ID WHERE T2.Store_Name  =  \"FJA Filming\"
SELECT T1.Claim_id ,  count(*) FROM Claims AS T1 JOIN Settlements AS T2 ON T1.claim_id  =  T2.claim_id GROUP BY T1.claim_id
SELECT T1.Code ,  T1.Fate ,  T2.Name FROM mission AS T1 JOIN ship AS T2 ON T1.Ship_ID  =  T2.Ship_ID
SELECT T1.Color FROM GRAPES AS T1 JOIN WINE AS T2 ON T1.Grape  =  T2.Grape GROUP BY T2.Grape ORDER BY AVG(Price) DESC LIMIT 1
SELECT T1.Committee FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T2.Party  =  \"Democratic\"
SELECT T1.Committee FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T2.Party  =  \"Democratic\" INTERSECT SELECT T1.Committee FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T2.Party  =  \"Liberal\"
SELECT T1.Company FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Height DESC LIMIT 1
SELECT T1.Continent FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode GROUP BY T1.Continent ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Country_name FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T2.Position  =  \"Forward\" INTERSECT SELECT T1.Country_name FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T2.Position  =  \"Defender\"
SELECT T1.County FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation GROUP BY T2.Appelation HAVING count(*)  <=  3
SELECT T1.County FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T2.Score  >  90 GROUP BY T1.County ORDER BY count(*) DESC LIMIT 1
SELECT T1.County_name ,  COUNT(*) FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District GROUP BY T1.County_id
SELECT T1.County_name FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District GROUP BY T1.County_id HAVING COUNT(*)  >=  2
SELECT T1.County_name FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District WHERE T2.Committee  =  \"Appropriations\"
SELECT T1.Customer_Event_ID  ,  T1.property_id FROM Customer_Events AS T1 JOIN Customer_Event_Notes AS T2 ON T1.Customer_Event_ID  =  T2.Customer_Event_ID GROUP BY T1.customer_event_id HAVING count(*) BETWEEN 1 AND 3
SELECT T1.DName FROM DEPARTMENT AS T1 JOIN MEMBER_OF AS T2 ON T1.DNO  =  T2.DNO GROUP BY T2.DNO ORDER BY count(*) ASC LIMIT 1
SELECT T1.DName FROM DEPARTMENT AS T1 JOIN MINOR_IN AS T2 ON T1.DNO  =  T2.DNO GROUP BY T2.DNO ORDER BY count(*) DESC LIMIT 1
SELECT T1.Date_Claim_Made ,  T1.Claim_id FROM Claims AS T1 JOIN Settlements AS T2 ON T1.Claim_id  =  T2.Claim_id GROUP BY T1.Claim_id HAVING count(*)  >  2 UNION SELECT T1.Date_Claim_Made ,  T1.Claim_id FROM Claims AS T1 JOIN Settlements AS T2 ON T1.Claim_id  =  T2.Claim_id WHERE T1.Amount_Claimed  =  ( SELECT max(Amount_Claimed) FROM Claims )
SELECT T1.Date_of_ceremony FROM music_festival AS T1 JOIN volume AS T2 ON T1.Volume  =  T2.Volume_ID WHERE T2.Weeks_on_Top  >  2
SELECT T1.Delegate ,  T2.Party FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID
SELECT T1.FacID ,  count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID
SELECT T1.FacID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID HAVING count(*)  >=  2
SELECT T1.Famous_Title FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T2.Weeks_on_Top  >  2
SELECT T1.Famous_Title FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T2.Weeks_on_Top  >  2 INTERSECT SELECT T1.Famous_Title FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T2.Weeks_on_Top  <  2
SELECT T1.Festival_ID ,  T3.Festival_Name ,  COUNT(*) FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID GROUP BY T1.Festival_ID
SELECT T1.Festival_ID ,  T3.Festival_Name FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID GROUP BY T1.Festival_ID HAVING COUNT(*)  >=  2
SELECT T1.FirstName ,  T1.SupportRepId FROM CUSTOMER AS T1 JOIN EMPLOYEE AS T2 ON T1.SupportRepId  =  T2.EmployeeId GROUP BY T1.SupportRepId HAVING COUNT(*)  >=  10
SELECT T1.FlightNo FROM FLIGHTS AS T1 JOIN AIRLINES AS T2 ON T2.uid  =  T1.Airline WHERE T2.Airline  =  \"United Airlines\"
SELECT T1.FlightNo FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.DestAirport   =  T2.AirportCode WHERE T2.City  =  \"Aberdeen\"
SELECT T1.FlightNo FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.SourceAirport   =  T2.AirportCode WHERE T2.City  =  \"Aberdeen\"
SELECT T1.Fname ,  T1.Lname FROM FACULTY AS T1 JOIN MEMBER_OF AS T2 ON T1.FacID  =  T2.FacID WHERE T2.DNO  =  520
SELECT T1.Fname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat' INTERSECT SELECT T1.Fname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'dog'
SELECT T1.Height FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Injured DESC
SELECT T1.Investor_details FROM  INVESTORS AS T1 JOIN TRANSACTIONS AS T2 ON T1.investor_id  =  T2.investor_id WHERE T2.share_count  >  100
SELECT T1.LastName FROM CUSTOMER AS T1 JOIN EMPLOYEE AS T2 ON T1.SupportRepId  =  T2.EmployeeId GROUP BY T1.SupportRepId HAVING COUNT(*)  <=  20
SELECT T1.Location ,  T2.Nickname FROM school AS T1 JOIN school_details AS T2 ON T1.School_ID  =  T2.School_ID
SELECT T1.Market_Details FROM Street_Markets AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Market_ID  =  T2.Tourist_Attraction_ID WHERE T2.How_to_Get_There  =  \"walk\" OR  T2.How_to_Get_There  =  \"bus\"
SELECT T1.Marketing_Region_Name FROM Marketing_Regions AS T1 JOIN Stores AS T2 ON T1.Marketing_Region_Code  =  T2.Marketing_Region_Code WHERE T2.Store_Name  =  \"Rob Dinning\"
SELECT T1.Museum_Details ,  T2.Opening_Hours FROM MUSEUMS AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Museum_ID  =  T2.Tourist_Attraction_ID
SELECT T1.Name ,  T1.Description FROM PHOTOS AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID WHERE T2.Name  =  \"film festival\"
SELECT T1.Name ,  T1.Population FROM city AS T1 JOIN countrylanguage AS T2 ON T1.CountryCode  =  T2.CountryCode WHERE T2.Language  =  \"English\" ORDER BY T1.Population DESC LIMIT 1
SELECT T1.Name ,  T1.Tourist_Attraction_ID FROM Tourist_Attractions AS T1 JOIN VISITS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID GROUP BY T2.Tourist_Attraction_ID HAVING count(*)  <=  1
SELECT T1.Name ,  T1.population FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID ORDER BY T2.Height DESC LIMIT 1
SELECT T1.Name ,  T2.Decoration_Theme FROM member AS T1 JOIN round AS T2 ON T1.Member_ID  =  T2.Member_ID
SELECT T1.Name ,  T2.Height FROM climber AS T1 JOIN mountain AS T2 ON T1.Mountain_ID  =  T2.Mountain_ID
SELECT T1.Name ,  T2.Name FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID
SELECT T1.Name ,  T2.Name FROM city AS T1 JOIN county_public_safety AS T2 ON T1.County_ID  =  T2.County_ID
SELECT T1.Name ,  T2.Name FROM climber AS T1 JOIN mountain AS T2 ON T1.Mountain_ID  =  T2.Mountain_ID
SELECT T1.Name ,  T2.Orchestra FROM conductor AS T1 JOIN orchestra AS T2 ON T1.Conductor_ID  =  T2.Conductor_ID
SELECT T1.Name ,  T2.Tourist_Attraction_ID ,   COUNT(*) FROM Tourist_Attractions AS T1 JOIN VISITS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID GROUP BY T2.Tourist_Attraction_ID
SELECT T1.Name ,  T2.Tourist_Attraction_ID FROM Tourist_Attractions AS T1 JOIN VISITS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID GROUP BY T2.Tourist_Attraction_ID HAVING count(*)  >=  2
SELECT T1.Name ,  T3.Name ,  T3.Hours FROM Scientists AS T1 JOIN AssignedTo AS T2 ON T1.SSN = T2.Scientist JOIN Projects AS T3 ON T2.Project = T3.Code ORDER BY T3.Name ,  T1.Name
SELECT T1.Name ,  avg(T2.Speed) FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID GROUP BY T1.Name
SELECT T1.Name ,  max(T1.Price) ,  T2.name FROM products AS T1 JOIN Manufacturers AS T2 ON T1.manufacturer  =  T2.code GROUP BY T2.name
SELECT T1.Name ,  sum(T2.Sales) FROM singer AS T1 JOIN song AS T2 ON T1.Singer_ID  =  T2.Singer_ID GROUP BY T1.Name
SELECT T1.Name FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId GROUP BY T2.GenreId ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Name FROM MEDIATYPE AS T1 JOIN TRACK AS T2 ON T1.MediaTypeId  =  T2.MediaTypeId GROUP BY T2.MediaTypeId ORDER BY COUNT(*) ASC LIMIT 1
SELECT T1.Name FROM STAFF AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID WHERE T2.Name  =  \"US museum\"
SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID  =  T3.Feature_ID WHERE T3.feature_Details  =  'park' UNION SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN Tourist_Attraction_Features AS T2 ON T1.tourist_attraction_id  =  T2.tourist_attraction_id JOIN Features AS T3 ON T2.Feature_ID  =  T3.Feature_ID WHERE T3.feature_Details  =  'shopping'
SELECT T1.Name FROM Tourist_Attractions AS T1 JOIN VISITS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID JOIN VISITORS AS T3 ON T2.Tourist_ID  =  T3.Tourist_ID WHERE T3.Tourist_Details  =  \"Vincent\"
SELECT T1.Name FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID ORDER BY T2.Year DESC
SELECT T1.Name FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID WHERE T2.Name  =  \"The Phantom of the Opera\"
SELECT T1.Name FROM conductor AS T1 JOIN orchestra AS T2 ON T1.Conductor_ID  =  T2.Conductor_ID GROUP BY T2.Conductor_ID HAVING COUNT(*)  >  1
SELECT T1.Name FROM conductor AS T1 JOIN orchestra AS T2 ON T1.Conductor_ID  =  T2.Conductor_ID GROUP BY T2.Conductor_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Name FROM conductor AS T1 JOIN orchestra AS T2 ON T1.Conductor_ID  =  T2.Conductor_ID WHERE Year_of_Founded  >  2008
SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode GROUP BY T1.Name ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" AND T2.IsOfficial  =  \"T\" INTERSECT SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"French\" AND T2.IsOfficial  =  \"T\"
SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" INTERSECT SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"French\"
SELECT T1.Name FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID GROUP BY T1.Name HAVING COUNT(*)  >  1
SELECT T1.Name FROM editor AS T1 JOIN journal_committee AS T2 ON T1.Editor_ID  =  T2.Editor_ID GROUP BY T1.Name HAVING COUNT(*)  >=  2
SELECT T1.Name FROM member AS T1 JOIN round AS T2 ON T1.Member_ID  =  T2.Member_ID ORDER BY Rank_in_Round ASC
SELECT T1.Name FROM member AS T1 JOIN round AS T2 ON T1.Member_ID  =  T2.Member_ID WHERE T2.Rank_in_Round  >  3
SELECT T1.Name FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID
SELECT T1.Name FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Weight DESC LIMIT 1
SELECT T1.Name FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Year DESC
SELECT T1.Name FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID WHERE T2.Country  =  \"China\" OR T2.Country  =  \"Japan\"
SELECT T1.Name FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID WHERE T2.Country != \"China\"
SELECT T1.Name FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID
SELECT T1.Name FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Earnings DESC
SELECT T1.Name FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Final_Table_Made
SELECT T1.Name FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID WHERE T2.Earnings  >  300000
SELECT T1.Name FROM singer AS T1 JOIN song AS T2 ON T1.Singer_ID  =  T2.Singer_ID GROUP BY T1.Name HAVING COUNT(*)  >  1
SELECT T1.Official_Name FROM city AS T1 JOIN farm_competition AS T2 ON T1.City_ID  =  T2.Host_city_ID GROUP BY T2.Host_city_ID HAVING COUNT(*)  >  1
SELECT T1.Official_native_language FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T2.College  =  \"Maryland\" OR T2.College  =  \"Duke\"
SELECT T1.Order_Date FROM Customer_Orders AS T1 JOIN ORDER_ITEMS AS T2 ON T1.Order_ID  =  T2.Order_ID JOIN Products AS T3 ON T2.Product_ID  =  T3.Product_ID WHERE T3.Product_price  >  1000
SELECT T1.Other_Item_Details FROM ORDER_ITEMS AS T1 JOIN Products AS T2 ON T1.Product_ID  =  T2.Product_ID WHERE T2.Product_price  >  2000
SELECT T1.Participant_ID ,  T1.Participant_Type_Code ,  count(*) FROM Participants AS T1 JOIN Participants_in_Events AS T2 ON T1.Participant_ID  =  T2.Participant_ID GROUP BY T1.Participant_ID
SELECT T1.Player , T1.Years_Played FROM player AS T1 JOIN team AS T2 ON T1.Team  =  T2.Team_id WHERE T2.Name  =  \"Columbus Crew\"
SELECT T1.Position FROM match_season AS T1 JOIN team AS T2 ON T1.Team  =  T2.Team_id WHERE T2.Name  =  \"Ryley Goldner\"
SELECT T1.Product_Name ,  COUNT(*) FROM Products AS T1 JOIN Products_in_Events AS T2 ON T1.Product_ID  =  T2.Product_ID GROUP BY T1.Product_Name
SELECT T1.Product_Name ,  COUNT(*) FROM Products AS T1 JOIN Products_in_Events AS T2 ON T1.Product_ID  =  T2.Product_ID GROUP BY T1.Product_Name ORDER BY COUNT(*) DESC
SELECT T1.Product_Name FROM Products AS T1 JOIN Products_in_Events AS T2 ON T1.Product_ID  =  T2.Product_ID GROUP BY T1.Product_Name HAVING COUNT(*)  >=  2
SELECT T1.Product_Name FROM Products AS T1 JOIN Products_in_Events AS T2 ON T1.Product_ID  =  T2.Product_ID GROUP BY T1.Product_Name HAVING COUNT(*)  >=  2 ORDER BY T1.Product_Name
SELECT T1.Result FROM acceptance AS T1 JOIN submission AS T2 ON T1.Submission_ID  =  T2.Submission_ID ORDER BY T2.Scores DESC LIMIT 1
SELECT T1.Royal_Family_Details ,  T2.How_to_Get_There FROM ROYAL_FAMILY AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Royal_Family_ID  =  T2.Tourist_Attraction_ID
SELECT T1.School_name ,  T1.Mascot ,  T1.IHSAA_Football_Class FROM school AS T1 JOIN budget AS T2 ON T1.school_id  =  T2.school_id WHERE Budgeted  >  6000 OR YEAR  <  2003 ORDER BY T2.total_budget_percent_invested ,  T2.total_budget_percent_budgeted
SELECT T1.Season ,  T1.Player ,  T2.Name FROM match_season AS T1 JOIN team AS T2 ON T1.Team  =  T2.Team_id
SELECT T1.Service_Type_Description ,  T2.Service_Type_Code ,  COUNT(*) FROM Ref_Service_Types AS T1 JOIN Services AS T2 ON T1.Service_Type_Code  =  T2.Service_Type_Code GROUP BY T2.Service_Type_Code
SELECT T1.Service_Type_Description , T1.Service_Type_Code FROM Ref_Service_Types AS T1 JOIN Services AS T2 ON T1.Service_Type_Code  =  T2.Service_Type_Code GROUP BY T1.Service_Type_Code ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Service_Type_Description FROM Ref_Service_Types AS T1 JOIN Services AS T2 ON T1.Service_Type_Code  =  T2.Service_Type_Code WHERE T2.Product_Name  =  'photo' INTERSECT SELECT T1.Service_Type_Description FROM Ref_Service_Types AS T1 JOIN Services AS T2 ON T1.Service_Type_Code  =  T2.Service_Type_Code WHERE T2.Product_Name  =  'film'
SELECT T1.Service_Type_Description FROM Ref_Service_Types AS T1 JOIN Services AS T2 ON T1.Service_Type_Code  =  T2.Service_Type_Code WHERE T2.Product_Price  >  100
SELECT T1.Shop_Details FROM SHOPS AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Shop_ID  =  T2.Tourist_Attraction_ID WHERE T2.How_to_Get_There  =  \"walk\"
SELECT T1.State_County FROM Addresses AS T1 JOIN Stores AS T2 ON T1.Address_ID  =  T2.Address_ID WHERE T2.Marketing_Region_Code  =  \"CA\"
SELECT T1.Status FROM city AS T1 JOIN farm_competition AS T2 ON T1.City_ID  =  T2.Host_city_ID GROUP BY T2.Host_city_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.Store_Phone ,  T1.Store_Email_Address FROM Drama_Workshop_Groups AS T1 JOIN Services AS T2 ON T1.Workshop_Group_ID  =  T2.Workshop_Group_ID
SELECT T1.Store_Phone ,  T1.Store_Email_Address FROM Drama_Workshop_Groups AS T1 JOIN Services AS T2 ON T1.Workshop_Group_ID  =  T2.Workshop_Group_ID WHERE T2.Product_Name  =  \"film\"
SELECT T1.Team ,  T2.Location FROM player AS T1 JOIN school AS T2 ON T1.School_ID  =  T2.School_ID
SELECT T1.Time FROM climber AS T1 JOIN mountain AS T2 ON T1.Mountain_ID  =  T2.Mountain_ID WHERE T2.Country  =  \"Uganda\"
SELECT T1.Time FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID ORDER BY T2.Days_held DESC LIMIT 1
SELECT T1.Time FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID WHERE T2.Days_held  >  50
SELECT T1.Title ,  T2.AlbumID ,  COUNT(*) FROM ALBUM AS T1 JOIN TRACK AS T2 ON T1.AlbumId  =  T2.AlbumId GROUP BY T2.AlbumID
SELECT T1.Title ,  T2.AlbumID FROM ALBUM AS T1 JOIN TRACK AS T2 ON T1.AlbumId  =  T2.AlbumId WHERE T2.UnitPrice  >  1 GROUP BY T2.AlbumID
SELECT T1.Title ,  T2.Publication_Date FROM book AS T1 JOIN publication AS T2 ON T1.Book_ID  =  T2.Book_ID
SELECT T1.Title ,  T2.Type FROM film AS T1 JOIN film_market_estimation AS T2 ON T1.Film_ID  =  T2.Film_ID
SELECT T1.Title FROM Album AS T1 JOIN Track AS T2 ON T1.AlbumId  =  T2.AlbumId JOIN Genre AS T3 ON T2.GenreID  =  T3.GenreID WHERE T3.Name  =  'Reggae' INTERSECT SELECT T1.Title FROM Album AS T1 JOIN Track AS T2 ON T1.AlbumId  =  T2.AlbumId JOIN Genre AS T3 ON T2.GenreID  =  T3.GenreID WHERE T3.Name  =  'Rock'
SELECT T1.Title FROM book AS T1 JOIN publication AS T2 ON T1.Book_ID  =  T2.Book_ID ORDER BY T2.Price DESC
SELECT T1.Total_Points FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID ORDER BY T2.Age ASC LIMIT 1
SELECT T1.White ,  T2.Crime_rate FROM city AS T1 JOIN county_public_safety AS T2 ON T1.County_ID  =  T2.County_ID
SELECT T1.Writer FROM book AS T1 JOIN publication AS T2 ON T1.Book_ID  =  T2.Book_ID WHERE T2.Price  >  4000000
SELECT T1.Year FROM film_market_estimation AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID WHERE T2.Country  =  \"Japan\" ORDER BY T1.Year DESC
SELECT T1.account_id ,  T1.date_account_opened ,  T1.account_name ,  T1.other_account_details FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.customer_first_name  =  'Meaghan'
SELECT T1.account_id ,  T2.account_name FROM Financial_transactions AS T1 JOIN Accounts AS T2 ON T1.account_id  =  T2.account_id GROUP BY T1.account_id HAVING count(*)  >=  4
SELECT T1.account_name ,  T1.other_account_details FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.customer_first_name  =  \"Meaghan\" AND T2.customer_last_name  =  \"Keeling\"
SELECT T1.activity_name ,  count(*) FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID
SELECT T1.activity_name FROM Activity AS T1 JOIN Faculty_participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1
SELECT T1.activity_name FROM Activity AS T1 JOIN Participates_in AS T2 ON T1.actID  =  T2.actID GROUP BY T1.actID ORDER BY count(*) DESC LIMIT 1
SELECT T1.address_details FROM addresses AS T1 JOIN customer_addresses AS T2 ON T1.address_id  =  T2.address_id WHERE T2.customer_id  =  10
SELECT T1.address_id ,  T1.line_1 ,  T1.line_2 FROM Addresses AS T1 JOIN Students AS T2 ON T1.address_id  =  T2.current_address_id GROUP BY T1.address_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.address_line_1 ,  T2.author_id FROM Course_Authors_and_Tutors AS T1 JOIN Courses AS T2 ON T1.author_id  =  T2.author_id GROUP BY T2.author_id HAVING Count(*)  >=  2
SELECT T1.address_line_1 FROM Course_Authors_and_Tutors AS T1 JOIN Courses AS T2 ON T1.author_id  =  T2.author_id WHERE T2.course_name  =  \"operating system\" OR T2.course_name  =  \"data structure\"
SELECT T1.age FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T2.name =  'Zach' AND T2.year  =  (SELECT max(YEAR) FROM PersonFriend WHERE name =  'Zach')
SELECT T1.amenity_name FROM dorm_amenity AS T1 JOIN has_amenity AS T2 ON T1.amenid  =  T2.amenid GROUP BY T2.amenid ORDER BY count(*) DESC LIMIT 1
SELECT T1.amenity_name FROM dorm_amenity AS T1 JOIN has_amenity AS T2 ON T2.amenid  =  T1.amenid JOIN dorm AS T3 ON T2.dormid  =  T3.dormid WHERE T3.dorm_name  =  'Anonymous Donor Hall' ORDER BY T1.amenity_name
SELECT T1.apt_number FROM Apartments AS T1 JOIN View_Unit_Status AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.available_yn  =  0 INTERSECT SELECT T1.apt_number FROM Apartments AS T1 JOIN View_Unit_Status AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.available_yn  =  1
SELECT T1.area_code FROM area_code_state AS T1 JOIN votes AS T2 ON T1.state  =  T2.state GROUP BY T1.area_code ORDER BY count(*) DESC LIMIT 1
SELECT T1.artist_name ,  T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name ORDER BY T2.rating DESC LIMIT 3
SELECT T1.artist_name ,  T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.resolution  >  900 GROUP BY T2.artist_name HAVING count(*)  >=  1
SELECT T1.artist_name ,  T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.song_name LIKE \"%love%\"
SELECT T1.artist_name ,  T1.gender FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.releasedate LIKE \"%Mar%\"
SELECT T1.artist_name ,  count(*) FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name GROUP BY T2.artist_name ORDER BY count(*) DESC LIMIT 3
SELECT T1.artist_name ,  count(*) FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.languages  =  \"english\" GROUP BY T2.artist_name HAVING count(*)  >=  1
SELECT T1.artist_name FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.languages  =  \"bangla\" GROUP BY T2.artist_name ORDER BY count(*) DESC LIMIT 3
SELECT T1.artist_name FROM song AS T1 JOIN files AS T2 ON T1.f_id  =  T2.f_id ORDER BY T2.duration LIMIT 1
SELECT T1.asset_id ,  T1.asset_details FROM Assets AS T1 JOIN Asset_Parts AS T2 ON T1.asset_id  =  T2.asset_id GROUP BY T1.asset_id HAVING count(*)  =  2 INTERSECT SELECT T1.asset_id ,  T1.asset_details FROM Assets AS T1 JOIN Fault_Log AS T2 ON T1.asset_id  =  T2.asset_id GROUP BY T1.asset_id HAVING count(*)  <  2
SELECT T1.balance + T2.balance FROM checking AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid ORDER BY T1.balance + T2.balance
SELECT T1.bname ,  T1.city FROM bank AS T1 JOIN loan AS T2 ON T1.branch_id  =  T2.branch_id WHERE T2.loan_type  =  'Business'
SELECT T1.bname FROM bank AS T1 JOIN loan AS T2 ON T1.branch_id  =  T2.branch_id GROUP BY T1.bname ORDER BY sum(T2.amount) DESC LIMIT 1
SELECT T1.booking_id ,  T1.amount_of_refund FROM Bookings AS T1 JOIN Payments AS T2 ON T1.booking_id  =  T2.booking_id GROUP BY T1.booking_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.booking_start_date ,  T1.booking_start_date FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.apt_type_code  =  \"Duplex\"
SELECT T1.booking_start_date ,  T1.booking_start_date FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.bedroom_count  >  2
SELECT T1.booking_start_date ,  T1.booking_start_date FROM Apartment_Bookings AS T1 JOIN Guests AS T2 ON T1.guest_id  =  T2.guest_id WHERE T2.gender_code  =  \"Female\"
SELECT T1.booking_status_code FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.apt_number  =  \"Suite 634\"
SELECT T1.breed_name FROM Breeds AS T1 JOIN Dogs AS T2 ON T1.breed_code  =  T2.breed_code GROUP BY T1.breed_name ORDER BY count(*) DESC LIMIT 1
SELECT T1.building_address FROM Apartment_Buildings AS T1 JOIN Apartments AS T2 ON T1.building_id  =  T2.building_id WHERE T2.bathroom_count  >  2
SELECT T1.cName FROM  tryout AS T1 JOIN player AS T2 ON T1.pID  =  T2.pID WHERE T2.pName LIKE 'D%'
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T1.county  =  \"Orange\" AND T2.year  >=  2000 GROUP BY T1.campus
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T2.year  >=  1998 AND T2.year  <=  2002 GROUP BY T1.campus
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND faculty  >  (SELECT max(faculty) FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND T1.county  =  \"Orange\")
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2003 ORDER BY T2.faculty DESC LIMIT 1
SELECT T1.campus FROM campuses AS t1 JOIN enrollments AS t2 ON t1.id  =  t2.campus WHERE t2.year  =  1956 AND totalenrollment_ay  >  400 AND FTE_AY  >  200
SELECT T1.campus FROM campuses AS t1 JOIN faculty AS t2 ON t1.id  =  t2.campus WHERE t2.faculty  >=  600 AND t2.faculty  <=  1000 AND T1.year  =  2004
SELECT T1.city FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id  =  T2.host_city GROUP BY T2.host_city ORDER BY count(*) DESC LIMIT 1
SELECT T1.city FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city WHERE T2.year  >  2010
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id ORDER BY T2.Feb DESC LIMIT 1
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Feb  >  T2.Jun UNION SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Mar  <  T2.Dec EXCEPT SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Mar  <  T2.Jul INTERSECT SELECT T3.city FROM city AS T3 JOIN hosting_city AS T4 ON T3.city_id  =  T4.host_city
SELECT T1.city FROM city AS T1 JOIN temperature AS T2 ON T1.city_id  =  T2.city_id WHERE T2.Mar  <  T2.Jul OR T2.Mar  >  T2.Oct
SELECT T1.claim_id ,  T1.date_claim_made ,  T1.Date_Claim_Settled FROM Claims AS T1 JOIN Settlements AS T2 ON T1.Claim_id  =  T2.Claim_id GROUP BY T1.claim_id HAVING count(*)  =  1
SELECT T1.claim_id ,  T1.date_claim_made ,  count(*) FROM Claims AS T1 JOIN Settlements AS T2 ON T1.claim_id  =  T2.claim_id GROUP BY T1.claim_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.club_id ,  T1.club_name, count(*) FROM club AS T1 JOIN coach AS T2 ON T1.club_id  =  T2.club_id GROUP BY T1.club_id
SELECT T1.club_id ,  T1.gold FROM match_result AS T1 JOIN coach AS T2 ON T1.club_id  =  T2.club_id GROUP BY T1.club_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.cmi_cross_ref_id ,  T1.source_system_code FROM CMI_Cross_References AS T1 JOIN Council_Tax AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id GROUP BY T1.cmi_cross_ref_id HAVING count(*)  >=  1
SELECT T1.cmi_details FROM Customer_Master_Index AS T1 JOIN CMI_Cross_References AS T2 ON T1.master_customer_id  =  T2.master_customer_id WHERE T2.source_system_code  =  'Tax'
SELECT T1.company_id ,  T1.company_name FROM Third_Party_Companies AS T1 JOIN Maintenance_Engineers AS T2 ON T1.company_id  =  T2.company_id GROUP BY T1.company_id HAVING count(*)  >=  2 UNION SELECT T3.company_id ,  T3.company_name FROM Third_Party_Companies AS T3 JOIN Maintenance_Contracts AS T4 ON T3.company_id  =  T4.maintenance_contract_company_id GROUP BY T3.company_id HAVING count(*)  >=  2
SELECT T1.company_name FROM Third_Party_Companies AS T1 JOIN Maintenance_Contracts AS T2 ON T1.company_id  =  T2.maintenance_contract_company_id JOIN Ref_Company_Types AS T3 ON T1.company_type_code  =  T3.company_type_code ORDER BY T2.contract_end_date DESC LIMIT 1
SELECT T1.company_name FROM Third_Party_Companies AS T1 JOIN Maintenance_Contracts AS T2 ON T1.company_id  =  T2.maintenance_contract_company_id ORDER BY T2.contract_start_date ASC LIMIT 1
SELECT T1.company_name FROM culture_company AS T1 JOIN book_club AS T2 ON T1.book_club_id  =  T2.book_club_id WHERE T2.publisher  =  'Alyson'
SELECT T1.compatible_since_year FROM accelerator_compatible_browser AS T1 JOIN browser AS T2 ON T1.browser_id  =  T2.id JOIN web_client_accelerator AS T3 ON T1.accelerator_id  =  T3.id WHERE T3.name  =  'CACHEbox' AND T2.name  =  'Internet Explorer'
SELECT T1.contestant_number , T1.contestant_name FROM contestants AS T1 JOIN votes AS T2 ON T1.contestant_number  =  T2.contestant_number GROUP BY T1.contestant_number HAVING count(*)  >=  2
SELECT T1.contestant_number , T1.contestant_name FROM contestants AS T1 JOIN votes AS T2 ON T1.contestant_number  =  T2.contestant_number GROUP BY T1.contestant_number ORDER BY count(*) ASC LIMIT 1
SELECT T1.cost_of_treatment ,  T2.treatment_type_description FROM Treatments AS T1 JOIN treatment_types AS T2 ON T1.treatment_type_code  =  T2.treatment_type_code
SELECT T1.council_tax_id FROM Rent_Arrears AS T1 JOIN CMI_Cross_References AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id JOIN Customer_Master_Index AS T3 ON T3.master_customer_id  =  T2.master_customer_id WHERE T3.cmi_details != 'Schmidt ,  Kertzmann and Lubowitz'
SELECT T1.country ,  T1.name ,  count(*) FROM airlines AS T1 JOIN routes AS T2 ON T1.alid  =  T2.alid GROUP BY T1.country ,  T1.name
SELECT T1.country FROM TV_Channel AS T1 JOIN cartoon AS T2 ON T1.id = T2.Channel WHERE T2.written_by  =  'Todd Casey'
SELECT T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name GROUP BY T2.artist_name ORDER BY count(*) LIMIT 1
SELECT T1.country FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T1.gender  =  \"Female\" AND T2.languages  =  \"bangla\"
SELECT T1.country_code ,  T1.first_name FROM players AS T1 JOIN matches AS T2 ON T1.player_id  =  T2.winner_id WHERE T2.tourney_name  =  'WTA Championships' INTERSECT SELECT T1.country_code ,  T1.first_name FROM players AS T1 JOIN matches AS T2 ON T1.player_id  =  T2.winner_id WHERE T2.tourney_name  =  'Australian Open'
SELECT T1.country_code ,  T1.first_name FROM players AS T1 JOIN rankings AS T2 ON T1.player_id  =  T2.player_id ORDER BY T2.tours DESC LIMIT 1
SELECT T1.coupon_amount FROM Discount_Coupons AS T1 JOIN customers AS T2 ON T1.coupon_id  =  T2.coupon_id WHERE T2.good_or_bad_customer  =  'good' INTERSECT SELECT T1.coupon_amount FROM Discount_Coupons AS T1 JOIN customers AS T2 ON T1.coupon_id  =  T2.coupon_id WHERE T2.good_or_bad_customer  =  'bad'
SELECT T1.course_description ,  T1.course_name FROM Courses AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_name HAVING COUNT(*)  >  2
SELECT T1.course_name ,  COUNT(*) FROM Courses AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_name
SELECT T1.course_name ,  T1.course_description FROM Courses AS T1 JOIN Subjects AS T2 ON T1.subject_id  =  T2.subject_id WHERE T2.subject_name  =  \"Computer Science\"
SELECT T1.course_name ,  T1.course_id FROM Courses AS T1 JOIN Sections AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_id HAVING count(*)  <=  2
SELECT T1.course_name FROM Courses AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_name HAVING COUNT(*)  =  1
SELECT T1.course_name FROM Courses AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.course_id  =  T2.course_id GROUP BY T1.course_name ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.course_name FROM courses AS T1 JOIN student_course_registrations AS T2 ON T1.course_id = T2.course_Id GROUP BY T1.course_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.crs_code FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN student AS T3 ON T3.stu_num  =  T2.stu_num WHERE T3.stu_lname  =  'Smithson'
SELECT T1.cust_name ,  T1.acc_type FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id GROUP BY T1.cust_name HAVING sum(T2.amount)  >  5000
SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id GROUP BY T1.cust_name HAVING count(*)  >  1
SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id GROUP BY T1.cust_name ORDER BY sum(T2.amount)
SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id GROUP BY T1.cust_name ORDER BY sum(T2.amount) DESC LIMIT 1
SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id WHERE amount  >  3000
SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id WHERE loan_type  =  'Mortgages' INTERSECT SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id WHERE loan_type  =  'Auto'
SELECT T1.custid ,  T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid WHERE T2.balance  <  (SELECT max(balance) FROM checking)
SELECT T1.customer_details ,  T1.customer_id FROM Customers AS T1 JOIN Customer_Policies AS T2 ON T1.Customer_id  =  T2.Customer_id GROUP BY T1.customer_id HAVING count(*)  >  1
SELECT T1.customer_details ,  T1.customer_id FROM Customers AS T1 JOIN Customer_Policies AS T2 ON T1.Customer_id  =  T2.customer_id GROUP BY T1.customer_id HAVING count(*)  >=  2 EXCEPT SELECT T1.customer_details ,  T1.customer_id FROM Customers AS T1 JOIN Customer_Policies AS T2 ON T1.Customer_id  =  T2.customer_id JOIN Claims AS T3 ON T2.policy_id  =  T3.policy_id
SELECT T1.customer_id ,  T1.customer_details FROM Customers AS T1 JOIN Customer_Events AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id HAVING count(*)  >=  3
SELECT T1.customer_id ,  T1.first_name ,  count(*) FROM Customers AS T1 JOIN bookings AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id
SELECT T1.customer_id ,  T2.customer_first_name ,  T2.customer_last_name ,  count(*) FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id
SELECT T1.customer_id ,  T2.customer_first_name ,  T2.customer_last_name FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_id ,  T2.customer_first_name ,  T2.customer_last_name FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id HAVING count(*)  >=  2
SELECT T1.customer_id ,  T2.customer_first_name ,  T2.customer_last_name FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) ASC LIMIT 1
SELECT T1.customer_id FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_id FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"On Road\" INTERSECT SELECT T1.customer_id FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"Shipped\"
SELECT T1.customer_name ,  T1.customer_address FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.order_status_code  =  \"New\" INTERSECT SELECT T1.customer_name ,  T1.customer_address FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.order_status_code  =  \"Pending\"
SELECT T1.customer_name ,  T1.customer_phone ,  T1.customer_email FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T2.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_name ,  T1.customer_phone FROM customers AS T1 JOIN mailshot_customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.outcome_code  =  'No Response'
SELECT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.payment_method_code  =  'Credit Card' GROUP BY T1.customer_id HAVING count(*)  >  2
SELECT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.order_status_code  =  'Completed' INTERSECT SELECT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.order_status_code  =  'Part'
SELECT T1.customer_name FROM customers AS T1 JOIN customer_orders AS T2 ON T1.customer_id  =  T2.customer_id WHERE shipping_method_code  =  'FedEx' AND order_status_code  =  'Paid'
SELECT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.date_order_placed >= \"2009-01-01\" AND T2.date_order_placed <= \"2010-01-01\"
SELECT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"On Road\" INTERSECT SELECT T1.customer_name FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T2.order_status = \"Shipped\"
SELECT T1.date ,  T2.player FROM game AS T1 JOIN injury_accident AS T2 ON T1.id  =  T2.game_id ORDER BY T1.season DESC
SELECT T1.date_in_location_from ,  T1.date_in_locaton_to FROM Document_locations AS T1 JOIN All_documents AS T2 ON T1.document_id  =  T2.document_id WHERE T2.document_name  =  \"Robin CV\"
SELECT T1.date_incident_start ,  date_incident_end FROM Behavior_Incident AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id WHERE T2.last_name  =  \"Fahey\"
SELECT T1.date_of_completion FROM Student_Course_Enrolment AS T1 JOIN Student_Tests_Taken AS T2 ON T1.registration_id  =  T2.registration_id WHERE T2.test_result  =  \"Fail\"
SELECT T1.date_of_enrolment ,  T1.date_of_completion FROM Student_Course_Enrolment AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id WHERE T2.family_name  =  \"Zieme\" AND T2.personal_name  =  \"Bernie\"
SELECT T1.date_of_enrolment ,  T1.date_of_completion FROM Student_Course_Enrolment AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id WHERE T2.personal_name  =  \"Karson\"
SELECT T1.date_of_enrolment FROM Student_Course_Enrolment AS T1 JOIN Student_Tests_Taken AS T2 ON T1.registration_id  =  T2.registration_id WHERE T2.test_result  =  \"Pass\"
SELECT T1.date_of_notes FROM Assessment_Notes AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id WHERE T2.first_name  =  \"Fanny\"
SELECT T1.date_of_treatment ,  T2.first_name FROM Treatments AS T1 JOIN Professionals AS T2 ON T1.professional_id  =  T2.professional_id
SELECT T1.date_order_placed FROM orders AS T1 JOIN shipments AS T2 ON T1.order_id = T2.order_id WHERE T2.invoice_number = 10
SELECT T1.date_order_placed FROM orders AS T1 JOIN shipments AS T2 ON T1.order_id = T2.order_id WHERE T2.shipment_tracking_number = 3452
SELECT T1.datetime_detention_start ,  datetime_detention_end FROM Detention AS T1 JOIN Teachers AS T2 ON T1.teacher_id  =  T2.teacher_id WHERE T2.last_name  =  \"Schultz\"
SELECT T1.degree_program_id ,  T1.degree_summary_name FROM Degree_Programs AS T1 JOIN Student_Enrolment AS T2 ON T1.degree_program_id  =  T2.degree_program_id GROUP BY T1.degree_program_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.degree_summary_name FROM Degree_Programs AS T1 JOIN Student_Enrolment AS T2 ON T1.degree_program_id  =  T2.degree_program_id GROUP BY T1.degree_summary_name ORDER BY count(*) DESC LIMIT 1
SELECT T1.department_id ,  T1.name ,  count(*) FROM management AS T2 JOIN department AS T1 ON T1.department_id  =  T2.department_id GROUP BY T1.department_id HAVING count(*)  >  1
SELECT T1.department_name ,  T2.city ,  T2.state_province FROM departments AS T1 JOIN locations AS T2 ON T2.location_id  =  T1.location_id
SELECT T1.detention_type_code ,  T2.detention_type_description FROM Detention AS T1 JOIN Ref_Detention_Type AS T2 ON T1.detention_type_code  =  T2.detention_type_code GROUP BY T1.detention_type_code ORDER BY count(*) ASC LIMIT 1
SELECT T1.director ,  T1.title FROM Movie AS T1 JOIN Rating AS T2 ON T1.mID  =  T2.mID WHERE T2.stars  =  5
SELECT T1.document_date FROM Documents AS T1 JOIN Documents_with_Expenses AS T2 ON T1.document_id  =  T2.document_id WHERE T2.budget_type_code  =  'GV' INTERSECT SELECT T1.document_date FROM Documents AS T1 JOIN Documents_with_Expenses AS T2 ON T1.document_id  =  T2.document_id WHERE T2.budget_type_code  =  'SF'
SELECT T1.document_id ,  T2.document_name ,  count(*) FROM Paragraphs AS T1 JOIN Documents AS T2 ON T1.document_id  =  T2.document_id GROUP BY T1.document_id
SELECT T1.document_id ,  T2.document_name FROM Paragraphs AS T1 JOIN Documents AS T2 ON T1.document_id  =  T2.document_id GROUP BY T1.document_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.document_id FROM Documents AS T1 JOIN Documents_with_expenses AS T2 ON T1.document_id  =  T2.document_id WHERE T1.document_name LIKE '%s%'
SELECT T1.document_id FROM Documents_with_expenses AS T1 JOIN Ref_Budget_Codes AS T2 ON T1.Budget_Type_code  =  T2.Budget_Type_code WHERE T2.budget_type_Description  =  \"Government\"
SELECT T1.document_type_name ,  T1.document_type_description ,  T2.Document_date FROM Ref_document_types AS T1 JOIN Documents AS T2 ON T1.document_type_code  =  T2.document_type_code
SELECT T1.donator_name FROM endowment AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T2.school_name  =  'Glenn' INTERSECT SELECT T1.donator_name FROM endowment AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T2.school_name  =  'Triton'
SELECT T1.dorm_name ,  T1.student_capacity FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid GROUP BY T2.dormid ORDER BY count(*) LIMIT 1
SELECT T1.dorm_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T3.amenity_name  =  'TV Lounge' EXCEPT SELECT T1.dorm_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T3.amenity_name  =  'Study Room'
SELECT T1.dorm_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T3.amenity_name  =  'TV Lounge' INTERSECT SELECT T1.dorm_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T3.amenity_name  =  'Study Room'
SELECT T1.driverid ,  T1.forename ,  T1.surname FROM drivers AS T1 JOIN laptimes AS T2 ON T1.driverid = T2.driverid WHERE POSITION  =  '1' GROUP BY T1.driverid HAVING count(*)  >=  2
SELECT T1.driverid ,  T1.forename ,  count(*) FROM drivers AS T1 JOIN results AS T2 ON T1.driverid = T2.driverid JOIN races AS T3 ON T2.raceid = T3.raceid GROUP BY T1.driverid HAVING count(*)  >=  2
SELECT T1.driverid ,  T1.surname FROM drivers AS T1 JOIN laptimes AS T2 ON T1.driverid = T2.driverid ORDER BY T2.milliseconds DESC LIMIT 1
SELECT T1.driverid ,  T1.surname FROM drivers AS T1 JOIN results AS T2 ON T1.driverid  =  T2.driverid JOIN races AS T3 ON T2.raceid = T3.raceid WHERE T3.year > 2010 GROUP BY T1.driverid ORDER BY count(*) DESC LIMIT 1
SELECT T1.driverid ,  T1.surname FROM drivers AS T1 JOIN results AS T2 ON T1.driverid = T2.driverid JOIN races AS T3 ON T2.raceid = T3.raceid GROUP BY T1.driverid ORDER BY count(*) DESC LIMIT 1
SELECT T1.driverid ,  count(*) FROM drivers AS T1 JOIN results AS T2 ON T1.driverid = T2.driverid JOIN races AS T3 ON T2.raceid = T3.raceid GROUP BY T1.driverid HAVING count(*)  <=  30
SELECT T1.duration ,  T1.file_size ,  T1.formats FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T2.genre_is  =  \"pop\" ORDER BY T2.song_name
SELECT T1.editor_id ,  T1.Name ,  COUNT(*) FROM editor AS T1 JOIN journal_committee AS T2 ON T1.Editor_ID  =  T2.Editor_ID GROUP BY T1.editor_id
SELECT T1.electoral_register_id FROM Electoral_Register AS T1 JOIN CMI_Cross_References AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id WHERE T2.source_system_code  =  'Electoral' OR T2.source_system_code  =  'Tax'
SELECT T1.emp_fname ,  T2.prof_office FROM employee AS T1 JOIN professor AS T2 ON T1.emp_num  =  T2.emp_num JOIN department AS T3 ON T3.dept_code  =  T2.dept_code WHERE T3.dept_name  =  'History' AND T2.prof_high_degree  =  'Ph.D.'
SELECT T1.emp_fname FROM employee AS T1 JOIN CLASS AS T2 ON T1.emp_num  =  T2.prof_num WHERE crs_code  =  'CIS-220' INTERSECT SELECT T1.emp_fname FROM employee AS T1 JOIN CLASS AS T2 ON T1.emp_num  =  T2.prof_num WHERE crs_code  =  'QM-261'
SELECT T1.emp_fname FROM employee AS T1 JOIN professor AS T2 ON T1.emp_num  =  T2.emp_num JOIN department AS T3 ON T2.dept_code  =  T3.dept_code WHERE T3.dept_name  =  'History' EXCEPT SELECT T4.emp_fname FROM employee AS T4 JOIN CLASS AS T5 ON T4.emp_num  =  T5.prof_num
SELECT T1.emp_lname ,  T1.emp_hiredate FROM employee AS T1 JOIN professor AS T2 ON T1.emp_num  =  T2.emp_num WHERE T2.prof_office  =  'DRE 102'
SELECT T1.emp_lname ,  T2.prof_office FROM employee AS T1 JOIN professor AS T2 ON T1.emp_num  =  T2.emp_num JOIN department AS T3 ON T2.dept_code  =  T3.dept_code WHERE T3.dept_name  =  'History'
SELECT T1.employee_id ,  T2.job_title FROM employees AS T1 JOIN jobs AS T2 ON T1.job_id  =  T2.job_id WHERE T1.department_id = 80
SELECT T1.employee_id ,  T4.country_name FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id JOIN locations AS T3 ON T2.location_id  =  T3.location_id JOIN countries AS T4 ON T3.country_id  =  T4.country_id
SELECT T1.employee_id FROM Employees AS T1 JOIN ROLES AS T2 ON T1.role_code  =  T2.role_code WHERE T2.role_name  =  \"Human Resource\" OR T2.role_name  =  \"Manager\"
SELECT T1.employee_name FROM Employees AS T1 JOIN ROLES AS T2 ON T1.role_code  =  T2.role_code WHERE T2.role_name  =  \"Editor\"
SELECT T1.engineer_id ,  T1.first_name ,  T1.last_name FROM Maintenance_Engineers AS T1 JOIN Engineer_Visits AS T2 ON T1.engineer_id  =  T2.engineer_id GROUP BY T1.engineer_id ORDER BY count(*) ASC LIMIT 1
SELECT T1.event_details FROM EVENTS AS T1 JOIN Services AS T2 ON T1.Service_ID  =  T2.Service_ID WHERE T2.Service_Type_Code  =  'Marriage'
SELECT T1.event_id ,  T1.event_details FROM EVENTS AS T1 JOIN Participants_in_Events AS T2 ON T1.Event_ID  =  T2.Event_ID GROUP BY T1.Event_ID HAVING count(*)  >  1
SELECT T1.facility_code FROM Apartment_Facilities AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T2.bedroom_count  >  4
SELECT T1.fault_description ,  T2.fault_status FROM Fault_Log AS T1 JOIN Fault_Log_Parts AS T2 ON T1.fault_log_entry_id  =  T2.fault_log_entry_id
SELECT T1.fault_log_entry_id ,  T1.fault_description ,  T1.fault_log_entry_datetime FROM Fault_Log AS T1 JOIN Fault_Log_Parts AS T2 ON T1.fault_log_entry_id  =  T2.fault_log_entry_id GROUP BY T1.fault_log_entry_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.fault_short_name ,  T3.skill_description FROM Part_Faults AS T1 JOIN Skills_Required_To_Fix AS T2 ON T1.part_fault_id  =  T2.part_fault_id JOIN Skills AS T3 ON T2.skill_id  =  T3.skill_id
SELECT T1.first_name ,  T1.country_code ,  T1.birth_date FROM players AS T1 JOIN matches AS T2 ON T1.player_id  =  T2.winner_id ORDER BY T2.winner_rank_points DESC LIMIT 1
SELECT T1.first_name ,  T1.job_id FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id WHERE T2.department_name  =  'Finance'
SELECT T1.first_name ,  T1.last_name ,  T1.customer_id FROM customer AS T1 JOIN payment AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY sum(amount) ASC LIMIT 1
SELECT T1.first_name ,  T1.last_name ,  T1.other_details ,  T3.skill_description FROM Maintenance_Engineers AS T1 JOIN Engineer_Skills AS T2 ON T1.engineer_id  =  T2.engineer_id JOIN Skills AS T3 ON T2.skill_id  =  T3.skill_id
SELECT T1.first_name ,  T1.last_name ,  T1.staff_id FROM staff AS T1 JOIN payment AS T2 ON T1.staff_id  =  T2.staff_id GROUP BY T1.staff_id ORDER BY count(*) ASC LIMIT 1
SELECT T1.first_name ,  T1.last_name ,  T2.department_name ,  T3.city ,  T3.state_province FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id JOIN locations AS T3 ON T2.location_id  =  T3.location_id
SELECT T1.first_name ,  T1.last_name ,  T2.size_code FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id
SELECT T1.first_name ,  T1.last_name ,  T3.city FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id JOIN locations AS T3 ON T2.location_id  =  T3.location_id WHERE T1.first_name LIKE '%z%'
SELECT T1.first_name ,  T1.last_name , T1.employee_id ,  T4.country_name FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id JOIN locations AS T3 ON T2.location_id  =  T3.location_id JOIN countries AS T4 ON T3.country_id  =  T4.country_id
SELECT T1.first_name ,  T1.last_name FROM customer AS T1 JOIN rental AS T2 ON T1.customer_id  =  T2.customer_id ORDER BY T2.rental_date ASC LIMIT 1
SELECT T1.first_name ,  T1.middle_name ,  T1.last_name ,  T1.student_id FROM Students AS T1 JOIN Student_Enrolment AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id HAVING count(*)  =  2
SELECT T1.first_name ,  T2.department_name FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id
SELECT T1.first_name ,  T2.name FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id
SELECT T1.first_name ,  T2.name FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id WHERE T1.state  =  'Virginia'
SELECT T1.first_name , T1.last_name FROM employees AS T1 JOIN customers AS T2 ON T1.id  =  T2.support_rep_id GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.firstname ,  T1.lastname FROM Reservations AS T1 JOIN Rooms AS T2 ON T1.Room  =  T2.RoomId WHERE T1.Rate - T2.basePrice  >  0
SELECT T1.firstname ,  T1.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"MARROTTE\" AND T2.lastname  =  \"KIRK\"
SELECT T1.firstname ,  T1.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"OTHA\" AND T2.lastname  =  \"MOYER\"
SELECT T1.flno FROM Flight AS T1 JOIN Aircraft AS T2 ON T1.aid  =  T2.aid WHERE T2.name  =  \"Airbus A340-300\"
SELECT T1.fname ,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'dog' AND T1.stuid NOT IN (SELECT T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat')
SELECT T1.fname ,  T1.age FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid WHERE T2.dormid NOT IN (SELECT T3.dormid FROM has_amenity AS T3 JOIN dorm_amenity AS T4 ON T3.amenid  =  T4.amenid WHERE T4.amenity_name  =  'TV Lounge')
SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.FacID ORDER BY count(*) DESC LIMIT 1
SELECT T1.fname ,  T1.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T2.fname  =  \"Linda\" AND T2.lname  =  \"Smith\"
SELECT T1.fname ,  T1.lname FROM Student AS T1 JOIN Participates_in AS T2 ON T1.StuID  =  T2.StuID GROUP BY T1.StuID ORDER BY count(*) DESC LIMIT 1
SELECT T1.fname ,  T1.lname FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid WHERE T2.dormid IN (SELECT T3.dormid FROM has_amenity AS T3 JOIN dorm_amenity AS T4 ON T3.amenid  =  T4.amenid WHERE T4.amenity_name  =  'TV Lounge')
SELECT T1.fname ,  T1.sex FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid GROUP BY T1.stuid HAVING count(*)  >  1
SELECT T1.fname FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid WHERE T3.dorm_name  =  'Smith Hall'
SELECT T1.fname FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid WHERE T2.dormid IN (SELECT T2.dormid FROM dorm AS T3 JOIN has_amenity AS T4 ON T3.dormid  =  T4.dormid JOIN dorm_amenity AS T5 ON T4.amenid  =  T5.amenid GROUP BY T3.dormid ORDER BY count(*) DESC LIMIT 1)
SELECT T1.forename ,  T1.surname ,  T1.driverid FROM drivers AS T1 JOIN pitstops AS T2 ON T1.driverid  =  T2.driverid GROUP BY T1.driverid HAVING count(*)  >  8 UNION SELECT T1.forename ,  T1.surname ,  T1.driverid FROM drivers AS T1 JOIN results AS T2 ON T1.driverid  =  T2.driverid GROUP BY T1.driverid HAVING count(*)  >  5
SELECT T1.forename ,  T1.surname FROM drivers AS T1 JOIN laptimes AS T2 ON T1.driverid = T2.driverid ORDER BY T2.milliseconds LIMIT 1
SELECT T1.gender ,  T1.artist_name FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name ORDER BY T2.resolution LIMIT 1
SELECT T1.good_or_bad_customer FROM customers AS T1 JOIN discount_coupons AS T2 ON T1.coupon_id  =  T2.coupon_id WHERE T2.coupon_amount  =  500
SELECT T1.grant_amount FROM Grants AS T1 JOIN Documents AS T2 ON T1.grant_id  =  T2.grant_id WHERE T2.sent_date  <  '1986-08-26 20:49:27' INTERSECT SELECT grant_amount FROM grants WHERE grant_end_date  >  '1989-03-16 18:27:16'
SELECT T1.grant_start_date FROM Grants AS T1 JOIN Documents AS T2 ON T1.grant_id  =  T2.grant_id JOIN Document_Types AS T3 ON T2.document_type_code  =  T3.document_type_code WHERE T3.document_description  =  'Regular' INTERSECT SELECT T1.grant_start_date FROM Grants AS T1 JOIN Documents AS T2 ON T1.grant_id  =  T2.grant_id JOIN Document_Types AS T3 ON T2.document_type_code  =  T3.document_type_code WHERE T3.document_description  =  'Initial Application'
SELECT T1.id ,  T1.Name ,  T1.FDA_approved FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id GROUP BY T1.id ORDER BY count(*) DESC
SELECT T1.id ,  T1.Name FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id GROUP BY T1.id HAVING count(*)  >=  2
SELECT T1.id ,  T1.Name FROM web_client_accelerator AS T1 JOIN accelerator_compatible_browser AS T2 ON T2.accelerator_id  =  T1.id GROUP BY T1.id HAVING count(*)  >=  2
SELECT T1.id ,  T1.name ,  T1.IATA FROM airport AS T1 JOIN flight AS T2 ON T1.id  =  T2.airport_id GROUP BY T2.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.id ,  T1.name ,  T1.gender FROM architect AS T1 JOIN bridge AS T2 ON T1.id  =  T2.architect_id GROUP BY T1.id HAVING count(*)  =  2 UNION SELECT T1.id ,  T1.name ,  T1.gender FROM architect AS T1 JOIN mill AS T2 ON T1.id  =  T2.architect_id GROUP BY T1.id HAVING count(*)  =  1
SELECT T1.id ,  T1.name ,  T1.nationality FROM architect AS T1 JOIN mill AS T2 ON T1.id  =  T2.architect_id GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.id ,  T1.name FROM architect AS T1 JOIN bridge AS T2 ON T1.id  =  T2.architect_id GROUP BY T1.id HAVING count(*)  >=  3
SELECT T1.id ,  T1.name FROM battle AS T1 JOIN ship AS T2 ON T1.id  =  T2.lost_in_battle JOIN death AS T3 ON T2.id  =  T3.caused_by_ship_id GROUP BY T1.id HAVING sum(T3.killed)  >  10
SELECT T1.id ,  T1.name FROM browser AS T1 JOIN accelerator_compatible_browser AS T2 ON T1.id  =  T2.browser_id GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.id ,  T1.name FROM enzyme AS T1 JOIN medicine_enzyme_interaction AS T2 ON T1.id  =  T2.enzyme_id WHERE T2.interaction_type  =  'activitor' GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.id ,  T1.name FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id GROUP BY T1.id HAVING count(*)  >=  2
SELECT T1.id ,  T1.name FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id WHERE T1.height  >  4000
SELECT T1.id ,  T1.name FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id GROUP BY T1.id HAVING count(*)  >  1
SELECT T1.id ,  T1.name FROM stadium AS T1 JOIN game AS T2 ON T1.id  =  T2.stadium_id JOIN injury_accident AS T3 ON T2.id  =  T3.game_id GROUP BY T1.id ORDER BY count(*) DESC LIMIT 1
SELECT T1.id ,  T1.score ,  T1.date FROM game AS T1 JOIN injury_accident AS T2 ON T2.game_id  =  T1.id GROUP BY T1.id HAVING count(*)  >=  2
SELECT T1.id ,  T1.trade_name FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id GROUP BY T1.id HAVING COUNT(*)  >=  3
SELECT T1.id ,  T2.installation_date FROM trip AS T1 JOIN station AS T2 ON T1.end_station_id  =  T2.id
SELECT T1.id ,  count(*) FROM stadium AS T1 JOIN game AS T2 ON T1.id  =  T2.stadium_id GROUP BY T1.id
SELECT T1.id FROM trip AS T1 JOIN station AS T2 ON T1.start_station_id  =  T2.id ORDER BY T2.dock_count DESC LIMIT 1
SELECT T1.id FROM trip AS T1 JOIN weather AS T2 ON T1.zip_code  =  T2.zip_code GROUP BY T2.zip_code HAVING avg(T2.mean_temperature_f)  >  60
SELECT T1.incident_type_code ,  T2.incident_type_description FROM Behavior_Incident AS T1 JOIN Ref_Incident_Type AS T2 ON T1.incident_type_code  =  T2.incident_type_code GROUP BY T1.incident_type_code ORDER BY count(*) DESC LIMIT 1
SELECT T1.institution ,  count(*) FROM institution AS T1 JOIN protein AS T2 ON T1.institution_id  =  T2.institution_id GROUP BY T1.institution_id
SELECT T1.interaction_type FROM medicine_enzyme_interaction AS T1 JOIN medicine AS T2 ON T1.medicine_id  =  T2.id JOIN enzyme AS T3 ON T1.enzyme_id  =  T3.id WHERE T3.name  =  'ALA synthase' AND T2.name  =  'Aripiprazole'
SELECT T1.invoice_date ,  T1.order_id ,  T2.order_details FROM Invoices AS T1 JOIN Orders AS T2 ON T1.order_id  =  T2.order_id
SELECT T1.killed ,  T1.injured FROM death AS T1 JOIN ship AS t2 ON T1.caused_by_ship_id  =  T2.id WHERE T2.tonnage  =  't'
SELECT T1.last_name FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id WHERE T2.age  =  ( SELECT max(age) FROM Dogs )
SELECT T1.last_name FROM Students AS T1 JOIN Addresses AS T2 ON T1.current_address_id  =  T2.address_id WHERE T2.state_province_county  =  'NorthCarolina' EXCEPT SELECT DISTINCT T3.last_name FROM Students AS T3 JOIN Student_Enrolment AS T4 ON T3.student_id  =  T4.student_id
SELECT T1.lat ,  T1.long ,  T1.city FROM station AS T1 JOIN trip AS T2 ON T1.id  =  T2.start_station_id ORDER BY T2.duration LIMIT 1
SELECT T1.line_1 ,  avg(T2.monthly_rental) FROM Addresses AS T1 JOIN Student_Addresses AS T2 ON T1.address_id  =  T2.address_id GROUP BY T2.address_id
SELECT T1.line_1 FROM Addresses AS T1 JOIN Students AS T2 ON T1.address_id  =  T2.address_id INTERSECT SELECT T1.line_1 FROM Addresses AS T1 JOIN Teachers AS T2 ON T1.address_id  =  T2.address_id
SELECT T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID JOIN activity AS T3 ON T2.actid  =  T2.actid WHERE T3.activity_name  =  'Canoeing' INTERSECT SELECT T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID JOIN activity AS T3 ON T2.actid  =  T2.actid WHERE T3.activity_name  =  'Kayaking'
SELECT T1.lname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pet_age  =  3 AND T3.pettype  =  'cat'
SELECT T1.login_name FROM Course_Authors_and_Tutors AS T1 JOIN Courses AS T2 ON T1.author_id  =  T2.author_id WHERE T2.course_name  =  \"advanced database\"
SELECT T1.lot_details FROM LOTS AS T1 JOIN TRANSACTIONS_LOTS AS T2 ON  T1.lot_id  =  T2.transaction_id JOIN TRANSACTIONS AS T3 ON T2.transaction_id  =  T3.transaction_id WHERE T3.share_count  >  100 AND T3.transaction_type_code  =  \"PUR\"
SELECT T1.lot_details FROM LOTS AS T1 JOIN TRANSACTIONS_LOTS AS T2 ON T1.lot_id  =  T2.transaction_id JOIN TRANSACTIONS AS T3 ON T2.transaction_id  =  T3.transaction_id WHERE T3.share_count  <  50
SELECT T1.member_name ,  T2.party_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id
SELECT T1.member_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id WHERE T2.Party_name != \"Progress Party\"
SELECT T1.member_name FROM member AS T1 JOIN party_events AS T2 ON T1.member_id  =  T2.member_in_charge_id GROUP BY T2.member_in_charge_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.model FROM CAR_NAMES AS T1 JOIN CARS_DATA AS T2 ON T1.MakeId  =  T2.Id WHERE T2.Weight  <  (SELECT avg(Weight) FROM CARS_DATA)
SELECT T1.name ,  T1.age ,  T1.job FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend =  'Alice' AND T2.year  =  (SELECT max(YEAR) FROM PersonFriend WHERE friend =  'Alice')
SELECT T1.name ,  T1.age FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend  =  'Dan' INTERSECT SELECT T1.name ,   T1.age FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend  =  'Alice'
SELECT T1.name ,  T1.area ,  T1.population FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID WHERE T2.speed  >  60 INTERSECT SELECT T1.name ,  T1.area ,  T1.population FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID WHERE T2.speed  <  55
SELECT T1.name ,  T1.city ,  T2.dst_apid FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.dst_apid GROUP BY T2.dst_apid ORDER BY count(*) DESC LIMIT 1
SELECT T1.name ,  T1.city ,  T2.src_apid FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.src_apid GROUP BY T2.src_apid ORDER BY count(*) DESC LIMIT 1
SELECT T1.name ,  T1.date ,  T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id
SELECT T1.name ,  T1.date FROM battle AS T1 JOIN ship AS T2 ON T1.id  =  T2.lost_in_battle WHERE T2.name  =  'Lettice' INTERSECT SELECT T1.name ,  T1.date FROM battle AS T1 JOIN ship AS T2 ON T1.id  =  T2.lost_in_battle WHERE T2.name  =  'HMS Atalanta'
SELECT T1.name ,  T1.email FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f1 GROUP BY T2.f1 HAVING count(*)  >  1
SELECT T1.name ,  T1.height_feet FROM building AS T1 JOIN institution AS T2 ON T1.building_id  =  T2.building_id WHERE T2.founded  >  1880 GROUP BY T1.building_id HAVING count(*)  >=  2
SELECT T1.name ,  T1.hours FROM projects AS T1 JOIN assignedto AS T2 ON T1.code  =  T2.project GROUP BY T2.project ORDER BY count(*) DESC LIMIT 1
SELECT T1.name ,  T1.id FROM station AS T1 JOIN status AS T2 ON T1.id  =  T2.station_id GROUP BY T2.station_id HAVING avg(T2.bikes_available)  >  14 UNION SELECT name ,  id FROM station WHERE installation_date LIKE \"12/%\"
SELECT T1.name ,  T1.lat ,  min(T2.duration) FROM station AS T1 JOIN trip AS T2 ON T1.id  =  T2.end_station_id GROUP BY T2.end_station_id
SELECT T1.name ,  T1.long ,  avg(T2.duration) FROM station AS T1 JOIN trip AS T2 ON T1.id  =  T2.start_station_id GROUP BY T2.start_station_id
SELECT T1.name ,  T1.max_speed FROM storm AS T1 JOIN affected_region AS T2 ON T1.storm_id  =  T2.storm_id GROUP BY T1.storm_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.name ,  T1.num_employees FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id WHERE T2.temporary_acting  =  'Yes'
SELECT T1.name ,  T1.partitionid FROM user_profiles AS T1 JOIN tweets AS T2 ON T1.uid  =  T2.uid GROUP BY T2.uid HAVING count(*)  <  2
SELECT T1.name ,  T1.salary FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid GROUP BY T1.eid ORDER BY count(*) DESC LIMIT 1
SELECT T1.name ,  T1.trade_name FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id WHERE interaction_type  =  'inhibitor' INTERSECT SELECT T1.name ,  T1.trade_name FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id WHERE interaction_type  =  'activitor'
SELECT T1.name ,  T1.type FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id
SELECT T1.name ,  T2.Player_id FROM club AS T1 JOIN player AS T2 ON T1.Club_ID  =  T2.Club_ID
SELECT T1.name ,  T2.alid FROM airlines AS T1 JOIN routes AS T2 ON T1.alid  =  T2.alid GROUP BY T2.alid ORDER BY count(*) DESC LIMIT 10
SELECT T1.name ,  T2.balance + T3.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid WHERE T3.balance  >  (SELECT avg(balance) FROM savings)
SELECT T1.name ,  T2.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid ORDER BY T3.balance LIMIT 1
SELECT T1.name ,  T2.balance FROM accounts AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid ORDER BY T2.balance DESC LIMIT 3
SELECT T1.name ,  T2.date_of_treatment FROM Dogs AS T1 JOIN Treatments AS T2 ON T1.dog_id  =  T2.dog_id WHERE T1.breed_code  =  ( SELECT breed_code FROM Dogs GROUP BY breed_code ORDER BY count(*) ASC LIMIT 1 )
SELECT T1.name ,  T2.name FROM Country AS T1 JOIN League AS T2 ON T1.id  =  T2.country_id
SELECT T1.name ,  T3.balance + T2.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid WHERE T3.balance  <  T2.balance
SELECT T1.name ,  T3.name FROM physician AS T1 JOIN affiliated_with AS T2 ON T1.EmployeeID  =  T2.physician JOIN department AS T3 ON T2.department  =  T3.DepartmentID WHERE T2.PrimaryAffiliation  =  1
SELECT T1.name ,  avg(T2.rating) FROM useracct AS T1 JOIN review AS T2 ON T1.u_id  =  T2.u_id GROUP BY T2.u_id
SELECT T1.name ,  avg(trust) FROM useracct AS T1 JOIN trust AS T2 ON T1.u_id  =  T2.target_u_id GROUP BY T2.target_u_id
SELECT T1.name ,  count(*) FROM building AS T1 JOIN institution AS T2 ON T1.building_id  =  T2.building_id GROUP BY T1.building_id
SELECT T1.name ,  count(*) FROM camera_lens AS T1 JOIN photos AS T2 ON T1.id  =  T2.camera_lens_id GROUP BY T1.id ORDER BY count(*)
SELECT T1.name ,  count(*) FROM physician AS T1 JOIN patient AS T2 ON T1.employeeid  =  T2.PCP GROUP BY T1.employeeid
SELECT T1.name ,  count(*) FROM storm AS T1 JOIN affected_region AS T2 ON T1.storm_id  =  T2.storm_id GROUP BY T1.storm_id
SELECT T1.name ,  count(*) FROM user_profiles AS T1 JOIN tweets AS T2 ON T1.uid  =  T2.uid GROUP BY T2.uid
SELECT T1.name ,  count(*) FROM user_profiles AS T1 JOIN tweets AS T2 ON T1.uid  =  T2.uid GROUP BY T2.uid HAVING count(*)  >  1
SELECT T1.name ,  count(*) FROM useracct AS T1 JOIN review AS T2 ON T1.u_id  =  T2.u_id GROUP BY T2.u_id
SELECT T1.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T3.distance  >  5000 GROUP BY T1.eid ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T3.name  =  \"Boeing 737-800\"
SELECT T1.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T3.name  =  \"Boeing 737-800\" INTERSECT SELECT T1.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T3.name  =  \"Airbus A340-300\"
SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend  =  'Bob'
SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend  =  'Zach' AND T1.gender  =  'female'
SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend IN (SELECT name FROM Person WHERE age  >  40) EXCEPT SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend IN (SELECT name FROM Person WHERE age  <  30)
SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend IN (SELECT name FROM Person WHERE age  >  40) INTERSECT SELECT T1.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T2.friend IN (SELECT name FROM Person WHERE age  <  30)
SELECT T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid ORDER BY T2.balance + T3.balance LIMIT 1
SELECT T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid WHERE T2.balance  >  T3.balance
SELECT T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid WHERE T2.balance  <  (SELECT avg(balance) FROM checking)
SELECT T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid WHERE T2.balance  >  (SELECT avg(balance) FROM checking) INTERSECT SELECT T1.name FROM accounts AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid WHERE T2.balance  <  (SELECT avg(balance) FROM savings)
SELECT T1.name FROM airlines AS T1 JOIN routes AS T2 ON T1.alid  =  T2.alid GROUP BY T1.name ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.dst_apid WHERE T1.country  =  'China' GROUP BY T1.name ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.src_apid WHERE T1.country  =  'China' GROUP BY T1.name ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM browser AS T1 JOIN accelerator_compatible_browser AS T2 ON T1.id  =  T2.browser_id JOIN web_client_accelerator AS T3 ON T2.accelerator_id  =  T3.id WHERE T3.name  =  'CProxy' AND T2.compatible_since_year  >  1998
SELECT T1.name FROM camera_lens AS T1 JOIN photos AS T2 ON T2.camera_lens_id  =  T1.id WHERE T1.brand  =  'Sigma' OR T1.brand  =  'Olympus'
SELECT T1.name FROM category AS T1 JOIN film_category AS T2 ON T1.category_id  =  T2.category_id JOIN film AS T3 ON T2.film_id  =  T3.film_id WHERE T3.title  =  'HUNGER ROOF'
SELECT T1.name FROM church AS T1 JOIN wedding AS T2 ON T1.church_id  =  T2.church_id GROUP BY T1.church_id HAVING count(*)  >=  2
SELECT T1.name FROM club AS T1 JOIN player AS T2 ON T1.Club_ID  =  T2.Club_ID WHERE T2.Position  =  \"Right Wing\"
SELECT T1.name FROM constructors AS T1 JOIN constructorstandings AS T2 ON T1.constructorid = T2.constructorid WHERE T1.nationality = \"Japanese\" AND T2.points > 5
SELECT T1.name FROM enzyme AS T1 JOIN medicine_enzyme_interaction AS T2 ON T1.id  =  T2.enzyme_id JOIN medicine AS T3 ON T2.medicine_id  =  T3.id WHERE T3.name  =  'Amisulpride' AND T2.interaction_type  =  'inhibitor'
SELECT T1.name FROM instructor AS T1 JOIN advisor AS T2 ON T1.id  =  T2.i_id GROUP BY T2.i_id HAVING count(*)  >  1
SELECT T1.name FROM instructor AS T1 JOIN teaches AS T2 ON T1.id  =  T2.id JOIN course AS T3 ON T2.course_id  =  T3.course_id WHERE T3.title  =  'C Programming'
SELECT T1.name FROM nurse AS T1 JOIN appointment AS T2 ON T1.employeeid  =  T2.prepnurse GROUP BY T1.employeeid ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM patient AS T1 JOIN appointment AS T2 ON T1.ssn = T2.patient ORDER BY T2.start DESC LIMIT 1
SELECT T1.name FROM physician AS T1 JOIN affiliated_with AS T2 ON T1.EmployeeID  =  T2.physician JOIN department AS T3 ON T2.department  =  T3.DepartmentID WHERE T3.name  =  'Surgery' INTERSECT SELECT T1.name FROM physician AS T1 JOIN affiliated_with AS T2 ON T1.EmployeeID  =  T2.physician JOIN department AS T3 ON T2.department  =  T3.DepartmentID WHERE T3.name  =  'Psychiatry'
SELECT T1.name FROM physician AS T1 JOIN affiliated_with AS T2 ON T1.EmployeeID  =  T2.physician JOIN department AS T3 ON T2.department  =  T3.DepartmentID WHERE T3.name  =  'Surgery' OR T3.name  =  'Psychiatry'
SELECT T1.name FROM physician AS T1 JOIN patient AS T2 ON T1.employeeid  =  T2.PCP GROUP BY T1.employeeid HAVING count(*)  >  1
SELECT T1.name FROM physician AS T1 JOIN prescribes AS T2 ON T1.employeeid = T2.physician ORDER BY T2.dose DESC LIMIT 1
SELECT T1.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment ORDER BY T3.cost DESC LIMIT 1
SELECT T1.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T3.cost  >  5000
SELECT T1.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code WHERE T2.name  =  'Creative Labs' INTERSECT SELECT T1.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code WHERE T2.name  =  'Sony'
SELECT T1.name FROM races AS T1 JOIN circuits AS T2 ON T1.circuitid = T2.circuitid WHERE T2.country = \"Spain\" AND T1.year > 2017
SELECT T1.name FROM station AS T1 JOIN status AS T2 ON T1.id  =  T2.station_id GROUP BY T2.station_id HAVING avg(bikes_available)  >  10 EXCEPT SELECT name FROM station WHERE city  =  \"San Jose\"
SELECT T1.name FROM storm AS T1 JOIN affected_region AS T2 ON T1.storm_id  =  T2.storm_id GROUP BY T1.storm_id HAVING count(*)  >=  2 INTERSECT SELECT T1.name FROM storm AS T1 JOIN affected_region AS T2 ON T1.storm_id  =  T2.storm_id GROUP BY T1.storm_id HAVING sum(T2.number_city_affected)  >=  10
SELECT T1.name FROM student AS T1 JOIN advisor AS T2 ON T1.id  =  T2.s_id GROUP BY T2.s_id HAVING count(*)  >  1
SELECT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id  =  T2.id WHERE T2.course_id IN (SELECT T4.prereq_id FROM course AS T3 JOIN prereq AS T4 ON T3.course_id  =  T4.course_id WHERE T3.title  =  'International Finance')
SELECT T1.name FROM tracks AS T1 JOIN playlist_tracks AS T2 ON T1.id  =  T2.track_id JOIN playlists AS T3 ON T2.playlist_id  =  T3.id WHERE T3.name  =  'Movies' EXCEPT SELECT T1.name FROM tracks AS T1 JOIN playlist_tracks AS T2 ON T1.id  =  T2.track_id JOIN playlists AS T3 ON T2.playlist_id  =  T3.id WHERE T3.name  =  'Music'
SELECT T1.name FROM tracks AS T1 JOIN playlist_tracks AS T2 ON T1.id  =  T2.track_id JOIN playlists AS T3 ON T2.playlist_id  =  T3.id WHERE T3.name  =  'Movies' INTERSECT SELECT T1.name FROM tracks AS T1 JOIN playlist_tracks AS T2 ON T1.id  =  T2.track_id JOIN playlists AS T3 ON T2.playlist_id  =  T3.id WHERE T3.name  =  'Music'
SELECT T1.name FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f1 GROUP BY T2.f1 HAVING count(*)  >  (SELECT count(*) FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f1 WHERE T1.name  =  'Tyler Swift')
SELECT T1.name FROM user_profiles AS T1 JOIN tweets AS T2 ON T1.uid  =  T2.uid GROUP BY T2.uid HAVING count(*)  >  1
SELECT T1.name FROM useracct AS T1 JOIN review AS T2 ON T1.u_id  =  T2.u_id GROUP BY T2.u_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.name FROM useracct AS T1 JOIN review AS T2 ON T1.u_id  =  T2.u_id ORDER BY T2.rating DESC LIMIT 1
SELECT T1.name FROM useracct AS T1 JOIN trust AS T2 ON T1.u_id  =  T2.source_u_id GROUP BY T2.source_u_id ORDER BY avg(trust) DESC LIMIT 1
SELECT T1.name FROM useracct AS T1 JOIN trust AS T2 ON T1.u_id  =  T2.target_u_id ORDER BY trust LIMIT 1
SELECT T1.order_id FROM orders AS T1 JOIN order_items AS T2 ON T1.order_id = T2.order_id GROUP BY T1.order_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.organisation_id ,  T1.organisation_type ,  T1.organisation_details FROM Organisations AS T1 JOIN Research_Staff AS T2 ON T1.organisation_id  =  T2.employer_organisation_id GROUP BY T1.organisation_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.organisation_id ,  count(*) FROM Projects AS T1 JOIN Project_outcomes AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.organisation_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.organisation_type ,  T1.organisation_id FROM Organisations AS T1 JOIN Research_Staff AS T2 ON T1.organisation_id  =  T2.employer_organisation_id GROUP BY T1.organisation_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.organisation_type FROM Organisations AS T1 JOIN Research_Staff AS T2 ON T1.organisation_id  =  T2.employer_organisation_id GROUP BY T1.organisation_type ORDER BY count(*) DESC LIMIT 1
SELECT T1.organisation_type_description FROM organisation_Types AS T1 JOIN Organisations AS T2 ON T1.organisation_type  =  T2.organisation_type WHERE T2.organisation_details  =  'quo'
SELECT T1.outcome_description FROM Research_outcomes AS T1 JOIN Project_outcomes AS T2 ON T1.outcome_code  =  T2.outcome_code
SELECT T1.outcome_description FROM Research_outcomes AS T1 JOIN Project_outcomes AS T2 ON T1.outcome_code  =  T2.outcome_code JOIN Projects AS T3 ON T2.project_id  =  T3.project_id WHERE T3.project_details  =  'sint'
SELECT T1.owner_id ,  T1.last_name FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id JOIN Treatments AS T3 ON T2.dog_id  =  T3.dog_id GROUP BY T1.owner_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.owner_id ,  T1.zip_code FROM Owners AS T1 JOIN Dogs AS T2 ON T1.owner_id  =  T2.owner_id JOIN Treatments AS T3 ON T2.dog_id  =  T3.dog_id GROUP BY T1.owner_id ORDER BY sum(T3.cost_of_treatment) DESC LIMIT 1
SELECT T1.owner_id ,  T2.first_name ,  T2.last_name FROM Dogs AS T1 JOIN Owners AS T2 ON T1.owner_id  =  T2.owner_id GROUP BY T1.owner_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.pName ,  T1.HS FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T2.decision  =  'yes'
SELECT T1.pName ,  T2.cName FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T2.decision  =  'yes'
SELECT T1.pName FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID ORDER BY T1.pName
SELECT T1.pName FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T2.decision  =  'yes' AND T2.pPos  =  'striker'
SELECT T1.paragraph_id ,   T1.paragraph_text FROM Paragraphs AS T1 JOIN Documents AS T2 ON T1.document_id  =  T2.document_id WHERE T2.Document_Name  =  'Welcome to NY'
SELECT T1.paragraph_text FROM Paragraphs AS T1 JOIN Documents AS T2 ON T1.document_id  =  T2.document_id WHERE T2.document_name  =  \"Customer reviews\"
SELECT T1.part_id ,  T1.part_name FROM Parts AS T1 JOIN Part_Faults AS T2 ON T1.part_id  =  T2.part_id JOIN Skills_Required_To_Fix AS T3 ON T2.part_fault_id  =  T3.part_fault_id GROUP BY T1.part_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.part_name ,  T1.part_id FROM Parts AS T1 JOIN Part_Faults AS T2 ON T1.part_id  =  T2.part_id GROUP BY T1.part_id HAVING count(*)  >  2
SELECT T1.part_name ,  count(*) FROM Parts AS T1 JOIN Asset_Parts AS T2 ON T1.part_id  =  T2.part_id GROUP BY T1.part_name
SELECT T1.part_name FROM Parts AS T1 JOIN Part_Faults AS T2 ON T1.part_id  =  T2.part_id GROUP BY T1.part_name ORDER BY count(*) ASC LIMIT 1
SELECT T1.party_name ,  T2.region_name FROM party AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id
SELECT T1.personal_name ,  T1.family_name ,  T2.author_id FROM Course_Authors_and_Tutors AS T1 JOIN Courses AS T2 ON T1.author_id  =  T2.author_id GROUP BY T2.author_id ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.problem_description FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE T2.staff_first_name = \"Christop\"
SELECT T1.problem_id FROM problems AS T1 JOIN product AS T2 ON T1.product_id = T2.product_id WHERE T2.product_name = \"voluptatem\" AND T1.date_problem_reported > \"1995\"
SELECT T1.problem_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE T2.staff_last_name = \"Bosco\"
SELECT T1.problem_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE date_problem_reported  <  ( SELECT min(date_problem_reported) FROM problems AS T3 JOIN staff AS T4 ON T3.reported_by_staff_id = T4.staff_id WHERE T4.staff_first_name = \"Lysanne\" AND T4.staff_last_name = \"Turcotte\" )
SELECT T1.problem_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE date_problem_reported  >  ( SELECT max(date_problem_reported) FROM problems AS T3 JOIN staff AS T4 ON T3.reported_by_staff_id = T4.staff_id WHERE T4.staff_first_name = \"Rylan\" AND T4.staff_last_name = \"Homenick\" )
SELECT T1.product_category_description FROM ref_product_categories AS T1 JOIN products AS T2 ON T1.product_category_code  =  T2.product_category_code WHERE T2.product_description LIKE '%t%'
SELECT T1.product_id FROM product_suppliers AS T1 JOIN products AS T2 ON T1.product_id  =  T2.product_id WHERE T1.supplier_id  =  2 AND T2.product_price  >  (SELECT avg(product_price) FROM products)
SELECT T1.product_name ,  T2.color_description ,  T1.product_description FROM products AS T1 JOIN Ref_colors AS T2 ON T1.color_code  =  T2.color_code WHERE product_category_code  =  \"Herbs\"
SELECT T1.product_name ,  sum(T2.order_quantity) FROM products AS T1 JOIN order_items AS T2 ON T1.product_id  =  T2.product_id GROUP BY T1.product_id
SELECT T1.product_name FROM products AS T1 JOIN ref_colors AS T2 ON T1.color_code  =  T2.color_code WHERE T2.color_description  =  'yellow'
SELECT T1.professional_id ,  T1.cell_number FROM Professionals AS T1 JOIN Treatments AS T2 ON T1.professional_id  =  T2.professional_id GROUP BY T1.professional_id HAVING count(*)  >=  2
SELECT T1.professional_id ,  T1.role_code ,  T1.first_name FROM Professionals AS T1 JOIN Treatments AS T2 ON T1.professional_id  =  T2.professional_id GROUP BY T1.professional_id HAVING count(*)  >=  2
SELECT T1.project_details ,  T1.project_id FROM Projects AS T1 JOIN Project_outcomes AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.project_details FROM Projects AS T1 JOIN Documents AS T2 ON T1.project_id  =  T2.project_id WHERE T2.document_name  =  \"King Book\"
SELECT T1.project_details FROM Projects AS T1 JOIN Project_outcomes AS T2 ON T1.project_id  =  T2.project_id JOIN Research_outcomes AS T3 ON T2.outcome_code  =  T3.outcome_code WHERE T3.outcome_description LIKE '%Published%'
SELECT T1.project_details FROM Projects AS T1 JOIN Project_outcomes AS T2 ON T1.project_id  =  T2.project_id WHERE T2.outcome_code  =  'Paper' INTERSECT SELECT T1.project_details FROM Projects AS T1 JOIN Project_outcomes AS T2 ON T1.project_id  =  T2.project_id WHERE T2.outcome_code  =  'Patent'
SELECT T1.project_id ,  T1.project_details FROM Projects AS T1 JOIN Documents AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id HAVING count(*)  >  2
SELECT T1.project_id ,  count(*) FROM Project_Staff AS T1 JOIN Projects AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id ORDER BY count(*) ASC
SELECT T1.property_id ,  count(*) FROM properties AS T1 JOIN residents AS T2 ON T1.property_id  =  T2.property_id GROUP BY T1.property_id
SELECT T1.purchase_details FROM PURCHASES AS T1 JOIN TRANSACTIONS AS T2 ON T1.purchase_transaction_id  =  T2.transaction_id WHERE T2.amount_of_transaction  >  10000
SELECT T1.rank ,  count(*) FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor GROUP BY T1.rank
SELECT T1.region_name ,  count(*) FROM region AS T1 JOIN affected_region AS T2 ON T1.region_id = T2.region_id GROUP BY T1.region_id
SELECT T1.region_name FROM region AS T1 JOIN affected_region AS T2 ON T1.region_id = T2.region_id GROUP BY T1.region_id HAVING count(*)  >=  2
SELECT T1.resident_id ,  T1.other_details ,  count(*) FROM Residents AS T1 JOIN Residents_Services AS T2 ON T1.resident_id  =  T2.resident_id GROUP BY T1.resident_id ORDER BY count(*) DESC
SELECT T1.response_received_date FROM Documents AS T1 JOIN Document_Types AS T2 ON T1.document_type_code  =  T2.document_type_code JOIN Grants AS T3 ON T1.grant_id  =  T3.grant_id WHERE T2.document_description  =  'Regular' OR T3.grant_amount  >  100
SELECT T1.role_description ,  T2.staff_id FROM Staff_Roles AS T1 JOIN Project_Staff AS T2 ON T1.role_code  =  T2.role_code JOIN Project_outcomes AS T3 ON T2.project_id  =  T3.project_id GROUP BY T2.staff_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.sales_details ,  T2.date_of_transaction FROM SALES AS T1 JOIN TRANSACTIONS AS T2 ON T1.sales_transaction_id  =  T2.transaction_id WHERE T2.amount_of_transaction  <  3000
SELECT T1.school_code FROM department AS T1 JOIN professor AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.school_code ORDER BY count(*) LIMIT 1
SELECT T1.season ,  T2.name FROM game AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.id JOIN injury_accident AS T3 ON T1.id  =  T3.game_id WHERE T3.injury  =  'Foot injury' OR T3.injury  =  'Knee problem'
SELECT T1.season FROM game AS T1 JOIN injury_accident AS T2 ON T1.id  =  T2.game_id WHERE T2.player  =  'Walter Samuel'
SELECT T1.semester_name ,  T1.semester_id FROM Semesters AS T1 JOIN Student_Enrolment AS T2 ON T1.semester_id  =  T2.semester_id GROUP BY T1.semester_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.sent_date FROM documents AS T1 JOIN Grants AS T2 ON T1.grant_id  =  T2.grant_id JOIN Organisations AS T3 ON T2.organisation_id  =  T3.organisation_id JOIN organisation_Types AS T4 ON T3.organisation_type  =  T4.organisation_type WHERE T2.grant_amount  >  5000 AND T4.organisation_type_description  =  'Research'
SELECT T1.series_name ,  T1.country FROM TV_Channel AS T1 JOIN cartoon AS T2 ON T1.id = T2.Channel WHERE T2.directed_by  =  'Michael Chang' INTERSECT SELECT T1.series_name ,  T1.country FROM TV_Channel AS T1 JOIN cartoon AS T2 ON T1.id = T2.Channel WHERE T2.directed_by  =  'Ben Jones'
SELECT T1.service_id ,  T1.service_details ,  count(*) FROM Services AS T1 JOIN Residents_Services AS T2 ON T1.service_id  =  T2.service_id GROUP BY T1.service_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.service_type_code FROM services AS T1 JOIN EVENTS AS T2 ON T1.service_id  =  T2.service_id WHERE T2.event_details  =  'Success' INTERSECT SELECT T1.service_type_code FROM services AS T1 JOIN EVENTS AS T2 ON T1.service_id  =  T2.service_id WHERE T2.event_details  =  'Fail'
SELECT T1.skill_id ,  T1.skill_description FROM Skills AS T1 JOIN Skills_Required_To_Fix AS T2 ON T1.skill_id  =  T2.skill_id GROUP BY T1.skill_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.source_system_code ,  T1.master_customer_id ,  T2.council_tax_id FROM CMI_Cross_References AS T1 JOIN Parking_Fines AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id
SELECT T1.source_system_code ,  T2.council_tax_id FROM CMI_Cross_References AS T1 JOIN Benefits_Overpayments AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id ORDER BY T2.council_tax_id
SELECT T1.staff_id ,  T1.staff_gender FROM staff AS T1 JOIN staff_department_assignments AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.job_title_code  =  \"Department Manager\"
SELECT T1.staff_id ,  T1.staff_name FROM staff AS T1 JOIN Staff_Department_Assignments AS T2 ON T1.staff_id  =  T2.staff_id ORDER BY date_assigned_to - date_assigned_from LIMIT 1
SELECT T1.staff_name ,  T1.staff_gender FROM staff AS T1 JOIN Staff_Department_Assignments AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.job_title_code  =  \"Sales Person\" EXCEPT SELECT T1.staff_name ,  T1.staff_gender FROM staff AS T1 JOIN Staff_Department_Assignments AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.job_title_code  =  \"Clerical Staff\"
SELECT T1.staff_name ,  T1.staff_gender FROM staff AS T1 JOIN staff_department_assignments AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.date_assigned_from LIKE \"2016%\"
SELECT T1.staff_name ,  T1.staff_id FROM Staff AS T1 JOIN Fault_Log AS T2 ON T1.staff_id  =  T2.recorded_by_staff_id EXCEPT SELECT T3.staff_name ,  T3.staff_id FROM Staff AS T3 JOIN Engineer_Visits AS T4 ON T3.staff_id  =  T4.contact_staff_id
SELECT T1.staff_name ,  T2.job_title_code FROM staff AS T1 JOIN staff_department_assignments AS T2 ON T1.staff_id  =  T2.staff_id ORDER BY T2.date_assigned_to DESC LIMIT 1
SELECT T1.staff_name ,  T3.first_name ,  T3.last_name FROM Staff AS T1 JOIN Engineer_Visits AS T2 ON T1.staff_id  =  T2.contact_staff_id JOIN Maintenance_Engineers AS T3 ON T2.engineer_id  =  T3.engineer_id
SELECT T1.staff_name ,  count(*) FROM Staff AS T1 JOIN Engineer_Visits AS T2 ON T1.staff_id  =  T2.contact_staff_id GROUP BY T1.staff_name
SELECT T1.staff_name FROM staff AS T1 JOIN staff_department_assignments AS T2 ON T1.staff_id  =  T2.staff_id GROUP BY T2.staff_id HAVING COUNT (*)  >  1
SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName JOIN player AS T3 ON T2.pID  =  T3.pID WHERE T3.pName  =  'Charles'
SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'goalie' INTERSECT SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'mid'
SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'mid' EXCEPT SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'goalie'
SELECT T1.state FROM college AS T1 JOIN tryout AS T2 ON T1.cName  =  T2.cName WHERE T2.pPos  =  'striker'
SELECT T1.statement_details ,  T2.document_name FROM Statements AS T1 JOIN Documents AS T2 ON T1.statement_id  =  T2.document_id WHERE T1.statement_details  =  'Private Project'
SELECT T1.statement_id ,  T2.statement_details ,  T1.account_details FROM Accounts AS T1 JOIN Statements AS T2 ON T1.statement_id  =  T2.statement_id
SELECT T1.statement_id ,  T2.statement_details FROM Accounts AS T1 JOIN Statements AS T2 ON T1.statement_id  =  T2.statement_id GROUP BY T1.statement_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.stu_fname ,  T1.stu_lname ,  T4.crs_description FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num JOIN CLASS AS T3 ON T2.class_code  =  T3.class_code JOIN course AS T4 ON T3.crs_code  =  T4.crs_code
SELECT T1.stu_fname ,  T1.stu_lname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num WHERE T2.enroll_grade  =  'C' OR T2.enroll_grade  =  'A'
SELECT T1.stu_fname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num GROUP BY T2.stu_num HAVING count(*)  =  1
SELECT T1.stu_fname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num JOIN CLASS AS T3 ON T2.class_code  =  T3.class_code JOIN course AS T4 ON T3.crs_code  =  T4.crs_code JOIN department AS T5 ON T5.dept_code  =  T4.dept_code WHERE T5.dept_name  =  'Accounting' INTERSECT SELECT T1.stu_fname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num JOIN CLASS AS T3 ON T2.class_code  =  T3.class_code JOIN course AS T4 ON T3.crs_code  =  T4.crs_code JOIN department AS T5 ON T5.dept_code  =  T4.dept_code WHERE T5.dept_name  =  'Computer Info. Systems'
SELECT T1.stu_fname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num JOIN CLASS AS T3 ON T2.class_code  =  T3.class_code WHERE T3.crs_code  =  'ACCT-211' AND T1.stu_lname LIKE 'S%'
SELECT T1.stu_lname FROM student AS T1 JOIN enroll AS T2 ON T1.stu_num  =  T2.stu_num WHERE T2.enroll_grade  =  'A' AND T2.class_code  =  10018
SELECT T1.student_details FROM students AS T1 JOIN student_course_registrations AS T2 ON T1.student_id = T2.student_id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.student_id ,  T1.first_name ,  T1.middle_name ,  T1.last_name ,  count(*) ,  T1.student_id FROM Students AS T1 JOIN Student_Enrolment AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.student_id ,  T2.course_name FROM student_course_registrations AS T1 JOIN courses AS T2 ON T1.course_id = T2.course_id
SELECT T1.student_id ,  T2.first_name FROM Assessment_Notes AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.student_id ,  T2.first_name FROM Student_Addresses AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id ORDER BY AVG(monthly_rental) DESC LIMIT 1
SELECT T1.student_id ,  T2.last_name FROM Behavior_Incident AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.student_id ,  T2.login_name FROM Student_Course_Enrolment AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.student_id ,  T2.middle_name FROM Student_Course_Enrolment AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id HAVING COUNT(*)  <=  2
SELECT T1.student_id ,  T2.personal_name FROM Student_Course_Enrolment AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id GROUP BY T1.student_id HAVING COUNT(*)  >=  2
SELECT T1.student_id ,  count(*) FROM students AS T1 JOIN student_course_registrations AS T2 ON T1.student_id = T2.student_id GROUP BY T1.student_id
SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T2.actid  =  T2.actid WHERE T2.activity_name  =  'Canoeing' INTERSECT SELECT T1.stuid FROM participates_in AS T1 JOIN activity AS T2 ON T2.actid  =  T2.actid WHERE T2.activity_name  =  'Kayaking'
SELECT T1.subject_id ,  T2.subject_name ,  COUNT(*) FROM Courses AS T1 JOIN Subjects AS T2 ON T1.subject_id  =  T2.subject_id GROUP BY T1.subject_id
SELECT T1.subject_id ,  T2.subject_name ,  COUNT(*) FROM Courses AS T1 JOIN Subjects AS T2 ON T1.subject_id  =  T2.subject_id GROUP BY T1.subject_id ORDER BY COUNT(*) ASC
SELECT T1.supplier_name ,  T1.supplier_phone FROM Suppliers AS T1 JOIN supplier_addresses AS T2 ON T1.supplier_id  =  T2.supplier_id JOIN addresses AS T3 ON T2.address_id  =  T3.address_id ORDER BY T3.address_details
SELECT T1.surname ,  T1.driverid FROM drivers AS T1 JOIN pitstops AS T2 ON T1.driverid  =  T2.driverid GROUP BY T1.driverid HAVING count(*)  =  11 INTERSECT SELECT T1.surname ,  T1.driverid FROM drivers AS T1 JOIN results AS T2 ON T1.driverid  =  T2.driverid GROUP BY T1.driverid HAVING count(*)  >  5
SELECT T1.task_details ,  T1.task_id ,  T2.project_id FROM Tasks AS T1 JOIN Projects AS T2 ON T1.project_id  =  T2.project_id WHERE T2.project_details  =  'omnis' UNION SELECT T1.task_details ,  T1.task_id ,  T2.project_id FROM Tasks AS T1 JOIN Projects AS T2 ON T1.project_id  =  T2.project_id JOIN Project_outcomes AS T3 ON T2.project_id  =  T3.project_id GROUP BY T2.project_id HAVING count(*)  >  2
SELECT T1.teacher_id ,  T2.first_name FROM Assessment_Notes AS T1 JOIN Teachers AS T2 ON T1.teacher_id  =  T2.teacher_id GROUP BY T1.teacher_id ORDER BY count(*) DESC LIMIT 3
SELECT T1.teacher_id ,  T2.last_name FROM Detention AS T1 JOIN Teachers AS T2 ON T1.teacher_id  =  T2.teacher_id WHERE T1.detention_type_code  =  \"AFTER\" GROUP BY T1.teacher_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.template_id ,  T2.Template_Type_Code FROM Documents AS T1 JOIN Templates AS T2 ON T1.template_id  =  T2.template_id GROUP BY T1.template_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.template_type_code ,  count(*) FROM Templates AS T1 JOIN Documents AS T2 ON T1.template_id  =  T2.template_id GROUP BY T1.template_type_code
SELECT T1.template_type_code FROM Templates AS T1 JOIN Documents AS T2 ON T1.template_id  =  T2.template_id GROUP BY T1.template_type_code ORDER BY count(*) DESC LIMIT 1
SELECT T1.template_type_code FROM Templates AS T1 JOIN Documents AS T2 ON T1.template_id  =  T2.template_id WHERE T2.document_name  =  \"Data base\"
SELECT T1.text_of_notes FROM Assessment_Notes AS T1 JOIN Teachers AS T2 ON T1.teacher_id  =  T2.teacher_id WHERE T2.last_name  =  \"Schuster\"
SELECT T1.theme ,  T2.name FROM exhibition AS T1 JOIN artist AS T2 ON T1.artist_id  =  T2.artist_id WHERE T1.ticket_price  >  (SELECT avg(ticket_price) FROM exhibition)
SELECT T1.thing_id ,  T1.type_of_Thing_Code ,  T2.organization_details FROM Things AS T1 JOIN Organizations AS T2 ON T1.organization_id  =  T2.organization_id
SELECT T1.title ,  T1.credits , T1.dept_name FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  >  1
SELECT T1.title ,  T1.director FROM Movie AS T1 JOIN Movie AS T2 ON T1.director  =  T2.director WHERE T1.title != T2.title ORDER BY T1.director ,  T1.title
SELECT T1.title ,  T1.film_id FROM film AS T1 JOIN inventory AS T2 ON T1.film_id  =  T2.film_id GROUP BY T1.film_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.title ,  T1.i_id FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id GROUP BY T2.i_id ORDER BY avg(T2.rank) DESC LIMIT 1
SELECT T1.title ,  T1.i_id FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id GROUP BY T2.i_id ORDER BY avg(T2.rating) DESC LIMIT 1
SELECT T1.title ,  T1.year FROM Movie AS T1 JOIN Movie AS T2 ON T1.director  =  T2.director WHERE T1.title != T2.title
SELECT T1.title ,  T2.inventory_id FROM film AS T1 JOIN inventory AS T2 ON T1.film_id  =  T2.film_id JOIN rental AS T3 ON T2.inventory_id  =  T3.inventory_id GROUP BY T2.inventory_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.title ,  T3.book_title FROM movie AS T1 JOIN culture_company AS T2 ON T1.movie_id  =  T2.movie_id JOIN book_club AS T3 ON T3.book_club_id  =  T2.book_club_id WHERE T2.incorporated_in  =  'China'
SELECT T1.title ,  T3.name FROM course AS T1 JOIN teaches AS T2 ON T1.course_id  =  T2.course_id JOIN instructor AS T3 ON T2.id  =  T3.id WHERE YEAR  =  2008 ORDER BY T1.title
SELECT T1.title FROM course AS T1 JOIN SECTION AS T2 ON T1.course_id  =  T2.course_id WHERE building  =  'Chandler' AND semester  =  'Fall' AND YEAR  =  2010
SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id GROUP BY T2.i_id ORDER BY avg(T2.rating) LIMIT 1
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rank  >  3 INTERSECT SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id GROUP BY T2.i_id HAVING avg(T2.rating)  >  5
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rating  <  5
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rating  =  10
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rating  >  (SELECT avg(rating) FROM review)
SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rating  >  8 INTERSECT SELECT T1.title FROM item AS T1 JOIN review AS T2 ON T1.i_id  =  T2.i_id WHERE T2.rating  <  5
SELECT T1.transaction_type_description ,  T2.date_of_transaction FROM Ref_Transaction_Types AS T1 JOIN TRANSACTIONS AS T2 ON T1.transaction_type_code  =  T2.transaction_type_code WHERE T2.share_count  <  10
SELECT T1.transaction_type_description FROM Ref_Transaction_Types AS T1 JOIN TRANSACTIONS AS T2 ON T1.transaction_type_code  =  T2.transaction_type_code GROUP BY T1.transaction_type_code ORDER BY COUNT(*) DESC LIMIT 1
SELECT T1.treatment_type_description FROM Treatment_types AS T1 JOIN Treatments AS T2 ON T1.treatment_type_code  =  T2.treatment_type_code GROUP BY T1.treatment_type_code ORDER BY sum(cost_of_treatment) ASC LIMIT 1
SELECT T1.vehicle_id ,  T1.vehicle_details FROM Vehicles AS T1 JOIN Lessons AS T2 ON T1.vehicle_id  =  T2.vehicle_id GROUP BY T1.vehicle_id ORDER BY count(*) DESC LIMIT 1
SELECT T1.zip_postcode FROM Addresses AS T1 JOIN Teachers AS T2 ON T1.address_id  =  T2.address_id WHERE T2.first_name  =  \"Lyla\"
SELECT T2.Age FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID ORDER BY T1.Total_Points DESC
SELECT T2.Author ,  COUNT(DISTINCT T1.workshop_id) FROM acceptance AS T1 JOIN submission AS T2 ON T1.Submission_ID  =  T2.Submission_ID GROUP BY T2.Author
SELECT T2.Author ,  T1.Result FROM acceptance AS T1 JOIN submission AS T2 ON T1.Submission_ID  =  T2.Submission_ID
SELECT T2.Author FROM acceptance AS T1 JOIN submission AS T2 ON T1.Submission_ID  =  T2.Submission_ID GROUP BY T2.Author HAVING COUNT(DISTINCT T1.workshop_id)  >  1
SELECT T2.Birth_Date ,  T2.Birth_Place FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Total DESC LIMIT 1
SELECT T2.Building FROM COURSE AS T1 JOIN FACULTY AS T2 ON T1.Instructor  =  T2.FacID GROUP BY T1.Instructor ORDER BY count(*) DESC LIMIT 1
SELECT T2.Carrier FROM stock AS T1 JOIN device AS T2 ON T1.Device_ID  =  T2.Device_ID GROUP BY T1.Device_ID HAVING COUNT(*)  >  1
SELECT T2.City FROM CUSTOMER AS T1 JOIN EMPLOYEE AS T2 ON T1.SupportRepId  =  T2.EmployeeId WHERE T1.PostalCode  =  \"70174\"
SELECT T2.Comptroller FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T1.District  =  1 OR T1.District  =  2
SELECT T2.Country ,  T1.Year FROM film_market_estimation AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID
SELECT T2.Crime_rate FROM city AS T1 JOIN county_public_safety AS T2 ON T1.County_ID  =  T2.County_ID WHERE T1.White  >  90
SELECT T2.Date FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Height DESC LIMIT 1
SELECT T2.Date_of_Birth FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Investor  =  \"Simon Woodroffe\" OR T1.Investor  =  \"Peter Jones\"
SELECT T2.Delegate ,  T1.County_name FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District
SELECT T2.Delegate FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District WHERE T1.Population  <  100000
SELECT T2.Denomination FROM player AS T1 JOIN school AS T2 ON T1.School_ID  =  T2.School_ID GROUP BY T1.School_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.Dname ,  T2.Room FROM COURSE AS T1 JOIN DEPARTMENT AS T2 ON T1.DNO  =  T2.DNO WHERE T1.CName  =  \"INTRODUCTION TO COMPUTER SCIENCE\"
SELECT T2.FirstName ,  T2.LastName FROM CUSTOMER AS T1 JOIN EMPLOYEE AS T2 ON T1.SupportRepId  =  T2.EmployeeId WHERE T1.FirstName  =  \"Leonie\"
SELECT T2.Fleet_Series FROM pilot_record AS T1 JOIN aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN pilot AS T3 ON T1.Pilot_ID  =  T3.Pilot_ID WHERE T3.Age  <  34
SELECT T2.Fname ,  T2.Lname FROM COURSE AS T1 JOIN FACULTY AS T2 ON T1.Instructor  =  T2.FacID GROUP BY T1.Instructor ORDER BY count(*) DESC LIMIT 3
SELECT T2.Fname ,  T2.Lname FROM COURSE AS T1 JOIN FACULTY AS T2 ON T1.Instructor  =  T2.FacID WHERE T1.CName  =  \"COMPUTER LITERACY\"
SELECT T2.Fname ,  T2.Lname FROM MINOR_IN AS T1 JOIN STUDENT AS T2 ON T1.StuID  =  T2.StuID WHERE T1.DNO  =  140
SELECT T2.Fname ,  T2.Lname FROM Sportsinfo AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID GROUP BY T1.StuID ORDER BY count(*) DESC LIMIT 1
SELECT T2.Governor FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T1.District  =  1
SELECT T2.Height FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Total  <  315
SELECT T2.Height FROM climber AS T1 JOIN mountain AS T2 ON T1.Mountain_ID  =  T2.Mountain_ID ORDER BY T1.Points DESC LIMIT 1
SELECT T2.Hometown ,  COUNT(*) FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID GROUP BY T2.Hometown
SELECT T2.Hometown FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID GROUP BY T2.Hometown HAVING COUNT(*)  >=  2
SELECT T2.Hometown FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID GROUP BY T2.Hometown ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.InvoiceDate FROM CUSTOMER AS T1 JOIN INVOICE AS T2 ON T1.CustomerId  =  T2.CustomerId WHERE T1.FirstName  =  \"Astrid\" AND LastName  =  \"Gruber\"
SELECT T2.Issue_Date FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T1.Artist  =  \"Gorgoroth\"
SELECT T2.Jan FROM city AS T1 JOIN temperature AS T2 ON T1.city_id = T2.city_id WHERE T1.city = \"Shanghai\"
SELECT T2.Language FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.Continent  =  \"Asia\" GROUP BY T2.Language ORDER BY COUNT (*) DESC LIMIT 1
SELECT T2.Language FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.GovernmentForm  =  \"Republic\" GROUP BY T2.Language HAVING COUNT(*)  =  1
SELECT T2.Language FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.HeadOfState  =  \"Beatrix\" AND T2.IsOfficial  =  \"T\"
SELECT T2.Language FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T1.Name  =  \"Aruba\" ORDER BY Percentage DESC LIMIT 1
SELECT T2.Lifespan FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID ORDER BY Vote_Percent DESC
SELECT T2.Lname FROM DEPARTMENT AS T1 JOIN FACULTY AS T2 ON T1.DNO  =  T3.DNO JOIN MEMBER_OF AS T3 ON T2.FacID  =  T3.FacID WHERE T1.DName  =  \"Computer Science\"
SELECT T2.Lname FROM Sportsinfo AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID WHERE T1.onscholarship  =  'Y'
SELECT T2.Location ,  T1.Aircraft FROM aircraft AS T1 JOIN MATCH AS T2 ON T1.Aircraft_ID  =  T2.Winning_Aircraft
SELECT T2.Location FROM player AS T1 JOIN school AS T2 ON T1.School_ID  =  T2.School_ID GROUP BY T1.School_ID HAVING COUNT(*)  >  1
SELECT T2.Money_Rank FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Height DESC LIMIT 1
SELECT T2.Name ,   T2.Score FROM GRAPES AS T1 JOIN WINE AS T2 ON T1.Grape  =  T2.Grape WHERE T1.Color  =  \"White\"
SELECT T2.Name ,  COUNT(*) FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID GROUP BY T1.Musical_ID
SELECT T2.Name ,  COUNT(*) FROM course_arrange AS T1 JOIN teacher AS T2 ON T1.Teacher_ID  =  T2.Teacher_ID GROUP BY T2.Name
SELECT T2.Name ,  COUNT(*) FROM debate_people AS T1 JOIN people AS T2 ON T1.Affirmative  =  T2.People_ID GROUP BY T2.Name
SELECT T2.Name ,  COUNT(*) FROM repair_assignment AS T1 JOIN technician AS T2 ON T1.technician_ID  =  T2.technician_ID GROUP BY T2.Name
SELECT T2.Name ,  T1.ArtistId FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistID GROUP BY T1.ArtistId HAVING COUNT(*)  >=  3 ORDER BY T2.Name
SELECT T2.Name ,  T1.College_Location FROM college AS T1 JOIN member AS T2 ON T1.College_ID  =  T2.College_ID ORDER BY T2.Name ASC
SELECT T2.Name ,  T1.Company FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Money_Requested
SELECT T2.Name ,  T1.Date FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID
SELECT T2.Name ,  T1.Elimination_Move FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID
SELECT T2.Name ,  T1.Location FROM railway AS T1 JOIN train AS T2 ON T1.Railway_ID  =  T2.Railway_ID
SELECT T2.Name ,  T1.Name FROM college AS T1 JOIN member AS T2 ON T1.College_ID  =  T2.College_ID
SELECT T2.Name ,  T1.Name FROM country AS T1 JOIN roller_coaster AS T2 ON T1.Country_ID  =  T2.Country_ID
SELECT T2.Name ,  T1.Team FROM elimination AS T1 JOIN wrestler AS T2 ON T1.Wrestler_ID  =  T2.Wrestler_ID ORDER BY T2.Days_held DESC
SELECT T2.Name ,  T2.age ,  T3.Theme FROM journal_committee AS T1 JOIN editor AS T2 ON T1.Editor_ID  =  T2.Editor_ID JOIN journal AS T3 ON T1.Journal_ID  =  T3.Journal_ID ORDER BY T3.Theme ASC
SELECT T2.Name ,  T3.Date FROM member_attendance AS T1 JOIN member AS T2 ON T1.Member_ID  =  T2.Member_ID JOIN performance AS T3 ON T1.Performance_ID  =  T3.Performance_ID ORDER BY T3.Attendance DESC
SELECT T2.Name ,  T3.Festival_Name FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID
SELECT T2.Name ,  T3.Location FROM member_attendance AS T1 JOIN member AS T2 ON T1.Member_ID  =  T2.Member_ID JOIN performance AS T3 ON T1.Performance_ID  =  T3.Performance_ID
SELECT T2.Name ,  T3.Location FROM member_attendance AS T1 JOIN member AS T2 ON T1.Member_ID  =  T2.Member_ID JOIN performance AS T3 ON T1.Performance_ID  =  T3.Performance_ID ORDER BY T2.Name ASC
SELECT T2.Name ,  T3.Theme FROM journal_committee AS T1 JOIN editor AS T2 ON T1.Editor_ID  =  T2.Editor_ID JOIN journal AS T3 ON T1.Journal_ID  =  T3.Journal_ID
SELECT T2.Name ,  sum(T1.Num_of_stock) FROM phone_market AS T1 JOIN phone AS T2 ON T1.Phone_ID  =  T2.Phone_ID GROUP BY T2.Name
SELECT T2.Name FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistId GROUP BY T2.Name ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.Name FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistId WHERE T1.Title  =  \"Balls to the Wall\"
SELECT T2.Name FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T1.County  =  \"Monterey\" AND T2.price  <  50
SELECT T2.Name FROM Grapes AS T1 JOIN WINE AS T2 ON T1.Grape  =  T2.Grape WHERE T1.Color  =  \"Red\" AND T2.price  >  50
SELECT T2.Name FROM Locations AS T1 JOIN Tourist_Attractions AS T2 ON T1.Location_ID  =  T2.Location_ID WHERE T1.Address  =  \"254 Ottilie Junction\" OR T2.How_to_Get_There  =  \"bus\"
SELECT T2.Name FROM Locations AS T1 JOIN Tourist_Attractions AS T2 ON T1.Location_ID  =  T2.Location_ID WHERE T1.Address  =  \"660 Shea Crescent\" OR T2.How_to_Get_There  =  \"walk\"
SELECT T2.Name FROM PHOTOS AS T1 JOIN TOURIST_ATTRACTIONS AS T2 ON T1.Tourist_Attraction_ID  =  T2.Tourist_Attraction_ID WHERE T1.Name  =  \"game1\"
SELECT T2.Name FROM actor AS T1 JOIN musical AS T2 ON T1.Musical_ID  =  T2.Musical_ID GROUP BY T1.Musical_ID HAVING COUNT(*)  >=  3
SELECT T2.Name FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID
SELECT T2.Name FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Total DESC
SELECT T2.Name FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Weight DESC LIMIT 1
SELECT T2.Name FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Total  >  300
SELECT T2.Name FROM course_arrange AS T1 JOIN teacher AS T2 ON T1.Teacher_ID  =  T2.Teacher_ID GROUP BY T2.Name HAVING COUNT(*)  >=  2
SELECT T2.Name FROM debate_people AS T1 JOIN people AS T2 ON T1.Negative  =  T2.People_ID GROUP BY T2.Name HAVING COUNT(*)  >=  2
SELECT T2.Name FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID ORDER BY votes DESC
SELECT T2.Name FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID WHERE Votes  >  10000
SELECT T2.Name FROM employment AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID JOIN company AS T3 ON T1.Company_ID  =  T3.Company_ID WHERE T3.Sales_in_Billion  >  200
SELECT T2.Name FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID
SELECT T2.Name FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T2.Weight DESC LIMIT 1
SELECT T2.Name FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Investor != \"Rachel Elnaugh\"
SELECT T2.Name FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID
SELECT T2.Name FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID ORDER BY T2.Height ASC
SELECT T2.Name FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID WHERE T2.Hometown != \"Santo Domingo\"
SELECT T2.Name FROM journal_committee AS T1 JOIN editor AS T2 ON T1.Editor_ID  =  T2.Editor_ID JOIN journal AS T3 ON T1.Journal_ID  =  T3.Journal_ID WHERE T3.Sales  >  3000
SELECT T2.Name FROM mission AS T1 JOIN ship AS T2 ON T1.Ship_ID  =  T2.Ship_ID WHERE T1.Launched_Year  >  1928
SELECT T2.Name FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID ORDER BY T3.Year
SELECT T2.Name FROM party_host AS T1 JOIN HOST AS T2 ON T1.Host_ID  =  T2.Host_ID JOIN party AS T3 ON T1.Party_ID  =  T3.Party_ID WHERE T3.Number_of_hosts  >  20
SELECT T2.Name FROM phone_market AS T1 JOIN phone AS T2 ON T1.Phone_ID  =  T2.Phone_ID GROUP BY T2.Name HAVING sum(T1.Num_of_stock)  >=  2000 ORDER BY sum(T1.Num_of_stock) DESC
SELECT T2.Party ,  COUNT(*) FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID GROUP BY T1.Party
SELECT T2.Party FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID GROUP BY T1.Party ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.Party FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T1.Committee  =  \"Appropriations\" INTERSECT SELECT T2.Party FROM election AS T1 JOIN party AS T2 ON T1.Party  =  T2.Party_ID WHERE T1.Committee  =  \"Economic Matters\"
SELECT T2.Party FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID ORDER BY votes ASC LIMIT 1
SELECT T2.Pilot_name ,  COUNT(*) FROM pilot_record AS T1 JOIN pilot AS T2 ON T1.pilot_ID  =  T2.pilot_ID GROUP BY T2.Pilot_name
SELECT T2.Pilot_name ,  COUNT(*) FROM pilot_record AS T1 JOIN pilot AS T2 ON T1.pilot_ID  =  T2.pilot_ID GROUP BY T2.Pilot_name HAVING COUNT(*)  >  1
SELECT T2.Player FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T1.Country_name  =  \"Indonesia\"
SELECT T2.Railway_ID ,  T1.Builder FROM railway AS T1 JOIN train AS T2 ON T1.Railway_ID  =  T2.Railway_ID GROUP BY T2.Railway_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.Railway_ID ,  T1.Location FROM railway AS T1 JOIN train AS T2 ON T1.Railway_ID  =  T2.Railway_ID GROUP BY T2.Railway_ID HAVING COUNT(*)  >  1
SELECT T2.School_name FROM endowment AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T1.amount  >  8.5 GROUP BY T1.school_id HAVING count(*)  >  1
SELECT T2.Season ,  T2.Player ,  T1.Country_name FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country
SELECT T2.Shop_Name FROM stock AS T1 JOIN shop AS T2 ON T1.Shop_ID  =  T2.Shop_ID GROUP BY T1.Shop_ID HAVING COUNT(*)  >  1
SELECT T2.Shop_Name FROM stock AS T1 JOIN shop AS T2 ON T1.Shop_ID  =  T2.Shop_ID GROUP BY T1.Shop_ID ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.Shop_Name FROM stock AS T1 JOIN shop AS T2 ON T1.Shop_ID  =  T2.Shop_ID GROUP BY T1.Shop_ID ORDER BY SUM(T1.quantity) DESC LIMIT 1
SELECT T2.Song FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T1.age  >=  32
SELECT T2.Song FROM music_festival AS T1 JOIN volume AS T2 ON T1.Volume  =  T2.Volume_ID WHERE T1.Result  =  \"Nominated\"
SELECT T2.Store_Email_Address FROM Addresses AS T1 JOIN Drama_Workshop_Groups AS T2 ON T1.Address_ID  =  T2.Address_ID WHERE T1.State_County  =  \"Alaska\"
SELECT T2.Store_Name FROM Addresses AS T1 JOIN Drama_Workshop_Groups AS T2 ON T1.Address_ID  =  T2.Address_ID WHERE T1.City_Town  =  \"Feliciaberg\"
SELECT T2.Store_Name FROM Bookings AS T1 JOIN Drama_Workshop_Groups AS T2 ON T1.Workshop_Group_ID  =  T2.Workshop_Group_ID WHERE T1.Status_Code  =  \"stop\"
SELECT T2.StuID FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T1.rank  =  \"Professor\"
SELECT T2.Theme FROM city AS T1 JOIN farm_competition AS T2 ON T1.City_ID  =  T2.Host_city_ID WHERE T1.Population  >  1000
SELECT T2.Title ,  T1.Name FROM singer AS T1 JOIN song AS T2 ON T1.Singer_ID  =  T2.Singer_ID
SELECT T2.Visit_Date ,  T2.Visit_Details FROM VISITORS AS T1 JOIN VISITS AS T2 ON T1.Tourist_ID  =  T2.Tourist_ID WHERE T1.Tourist_Details  =  \"Vincent\"
SELECT T2.Weight FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID ORDER BY T1.Money_Requested DESC
SELECT T2.Winery FROM GRAPES AS T1 JOIN WINE AS T2 ON T1.GRAPE  =  T2.GRAPE WHERE T1.Color  =  \"White\" GROUP BY T2.Winery ORDER BY count(*) DESC LIMIT 3
SELECT T2.Year ,  T1.Official_Name FROM city AS T1 JOIN farm_competition AS T2 ON T1.City_ID  =  T2.Host_city_ID
SELECT T2.account_name ,  T1.account_id ,  count(*) FROM Financial_transactions AS T1 JOIN Accounts AS T2 ON T1.account_id  =  T2.account_id GROUP BY T1.account_id
SELECT T2.address ,  T1.email FROM customer AS T1 JOIN address AS T2 ON T2.address_id  =  T1.address_id WHERE T1.first_name  =  'LINDA'
SELECT T2.address FROM staff AS T1 JOIN address AS T2 ON T1.address_id  =  T2.address_id WHERE T1.first_name  =  'Elsa'
SELECT T2.address FROM store AS T1 JOIN address AS T2 ON T1.address_id  =  T2.address_id WHERE store_id  =  1
SELECT T2.address_id ,  T1.city FROM Addresses AS T1 JOIN Student_Addresses AS T2 ON T1.address_id  =  T2.address_id GROUP BY T2.address_id ORDER BY AVG(monthly_rental) DESC LIMIT 1
SELECT T2.address_id ,  T1.zip_postcode FROM Addresses AS T1 JOIN Student_Addresses AS T2 ON T1.address_id  =  T2.address_id ORDER BY monthly_rental DESC LIMIT 1
SELECT T2.allergytype ,  count(*) FROM Has_allergy AS T1 JOIN Allergy_type AS T2 ON T1.allergy  =  T2.allergy GROUP BY T2.allergytype
SELECT T2.apt_number ,  T1.booking_start_date ,  T1.booking_start_date FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id
SELECT T2.apt_number FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T1.booking_status_code  =  \"Confirmed\" INTERSECT SELECT T2.apt_number FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T1.booking_status_code  =  \"Provisional\"
SELECT T2.apt_type_code ,  T2.apt_number FROM Apartment_Buildings AS T1 JOIN Apartments AS T2 ON T1.building_id  =  T2.building_id WHERE T1.building_manager  =  \"Kyle\"
SELECT T2.balance ,  T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid ORDER BY T3.balance LIMIT 1
SELECT T2.balance ,  T3.balance ,  T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid
SELECT T2.balance ,  T3.balance ,  T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid ORDER BY T2.balance + T3.balance DESC
SELECT T2.balance ,  T3.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid WHERE T1.name  =  'Brown'
SELECT T2.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid WHERE T1.name IN (SELECT T1.name FROM accounts AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid WHERE T2.balance  >  (SELECT avg(balance) FROM savings))
SELECT T2.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid WHERE T1.name LIKE '%ee%'
SELECT T2.bname FROM loan AS T1 JOIN bank AS T2 ON T1.branch_id  =  T2.branch_id JOIN customer AS T3 ON T1.cust_id  =  T3.cust_id WHERE T3.credit_score  <  100
SELECT T2.bname FROM loan AS T1 JOIN bank AS T2 ON T1.branch_id  =  T2.branch_id JOIN customer AS T3 ON T1.cust_id  =  T3.cust_id WHERE T3.credit_score  <  100 GROUP BY T2.bname ORDER BY sum(T1.amount) DESC LIMIT 1
SELECT T2.budget_type_code ,  T2.budget_type_description ,  T1.document_id FROM Documents_with_expenses AS T1 JOIN Ref_budget_codes AS T2 ON T1.budget_type_code  =  T2.budget_type_code
SELECT T2.building ,  T2.room_number ,  T2.semester ,  T2.year FROM course AS T1 JOIN SECTION AS T2 ON T1.course_id  =  T2.course_id WHERE T1.dept_name  =  'Psychology' ORDER BY T1.title
SELECT T2.candidate_id FROM people AS T1 JOIN candidates AS T2 ON T1.person_id = T2.candidate_id WHERE T1.email_address = \"stanley.monahan@example.org\"
SELECT T2.card_type_code ,  count(*) FROM Financial_transactions AS T1 JOIN Customers_cards AS T2 ON T1.card_id  =  T2.card_id GROUP BY T2.card_type_code
SELECT T2.cell_mobile_number FROM Student_Addresses AS T1 JOIN Students AS T2 ON T1.student_id  =  T2.student_id ORDER BY T1.monthly_rental ASC LIMIT 1
SELECT T2.city ,  count(*) ,  T1.city_id FROM address AS T1 JOIN city AS T2 ON T1.city_id  =  T2.city_id GROUP BY T1.city_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.cmi_cross_ref_id ,  T2.master_customer_id ,  count(*) FROM Business_Rates AS T1 JOIN CMI_Cross_References AS T2 ON T1.cmi_cross_ref_id  =  T2.cmi_cross_ref_id GROUP BY T2.cmi_cross_ref_id
SELECT T2.company ,  count(*) FROM station_company AS T1 JOIN company AS T2 ON T1.company_id  =  T2.company_id GROUP BY T1.company_id
SELECT T2.company_name FROM movie AS T1 JOIN culture_company AS T2 ON T1.movie_id  =  T2.movie_id WHERE T1.year  =  1999
SELECT T2.concert_name ,  T2.theme ,  count(*) FROM singer_in_concert AS T1 JOIN concert AS T2 ON T1.concert_id  =  T2.concert_id GROUP BY T2.concert_id
SELECT T2.country FROM Customers AS T1 JOIN Addresses AS T2 ON T1.customer_address_id = T2.address_id WHERE T1.first_name = \"Carole\" AND T1.last_name = \"Bernhard\"
SELECT T2.country FROM city AS T1 JOIN country AS T2 ON T1.country_id  =  T2.country_id GROUP BY T2.country_id HAVING count(*)  >=  3
SELECT T2.course_name FROM Course_Authors_and_Tutors AS T1 JOIN Courses AS T2 ON T1.author_id  =  T2.author_id WHERE T1.personal_name  =  \"Julio\"
SELECT T2.created ,  T2.state ,  T2.phone_number FROM contestants AS T1 JOIN votes AS T2 ON T1.contestant_number  =  T2.contestant_number WHERE T1.contestant_name  =  'Tabatha Gehling'
SELECT T2.customer_first_name ,  T1.customer_id FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id HAVING count(*)  >=  2
SELECT T2.customer_first_name ,  T2.customer_last_name ,  T1.customer_id FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) ASC LIMIT 1
SELECT T2.customer_first_name ,  T2.customer_last_name ,  T2.customer_phone FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.account_name  =  \"162\"
SELECT T2.customer_first_name ,  T2.customer_last_name ,  T2.customer_phone FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.card_number  =  \"4560596484842\"
SELECT T2.customer_first_name ,  T2.customer_last_name FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.account_name  =  \"900\"
SELECT T2.customer_last_name ,  T1.customer_id ,  T2.phone_number FROM Orders AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.customer_name ,  T2.payment_method FROM mailshot_customers AS T1 JOIN customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.outcome_code  =  'Order' INTERSECT SELECT T2.customer_name ,  T2.payment_method FROM mailshot_customers AS T1 JOIN customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T1.outcome_code  =  'No Response'
SELECT T2.customer_name ,  T3.city ,  T1.date_from ,  T1.date_to FROM customer_address_history AS T1 JOIN customers AS T2 ON T1.customer_id  =  T2.customer_id JOIN addresses AS T3 ON T1.address_id  =  T3.address_id
SELECT T2.customer_name ,  count(*) FROM orders AS T1 JOIN customers AS T2 ON T1.customer_id = T2.customer_id GROUP BY T2.customer_id HAVING count(*)  >=  2
SELECT T2.customer_name FROM mailshot_customers AS T1 JOIN customers AS T2 ON T1.customer_id  =  T2.customer_id GROUP BY T1.customer_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.customer_name FROM mailshot_customers AS T1 JOIN customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE outcome_code  =  'Order' GROUP BY T1.customer_id HAVING count(*)  >=  2
SELECT T2.customer_name FROM orders AS T1 JOIN customers AS T2 ON T1.customer_id = T2.customer_id GROUP BY T2.customer_id HAVING count(*)  <=  2
SELECT T2.date_moved_in ,  T1.customer_id ,  T1.customer_details FROM Customers AS T1 JOIN Customer_Events AS T2 ON T1.customer_id  =  T2.customer_id
SELECT T2.date_of_enrolment FROM Courses AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.course_id  =  T2.course_id WHERE T1.course_name  =  \"Spanish\"
SELECT T2.date_order_placed FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T1.customer_name = \"Jeramie\"
SELECT T2.day_Number ,  T1.Date_Stored FROM All_documents AS T1 JOIN Ref_calendar AS T2 ON T1.date_stored  =  T2.calendar_date
SELECT T2.department_name ,  COUNT(*) FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id GROUP BY T2.department_name
SELECT T2.department_name ,  T1.department_id FROM Degree_Programs AS T1 JOIN Departments AS T2 ON T1.department_id  =  T2.department_id GROUP BY T1.department_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.dept_name ,  T1.dept_code FROM professor AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code WHERE T1.prof_high_degree  =  'Ph.D.' GROUP BY T1.dept_code ORDER BY count(*) DESC LIMIT 1
SELECT T2.dept_name ,  T2.dept_address ,  count(*) FROM student AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.dept_code ORDER BY count(*) DESC LIMIT 3
SELECT T2.dept_name ,  T2.dept_address FROM student AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.dept_code ORDER BY count(*) DESC LIMIT 1
SELECT T2.dept_name ,  avg(T1.stu_gpa) FROM student AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.dept_code ORDER BY avg(T1.stu_gpa) DESC LIMIT 1
SELECT T2.dept_name FROM course AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code WHERE T1.crs_description LIKE '%Statistics%'
SELECT T2.dept_name FROM professor AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.dept_code ORDER BY count(*) LIMIT 1
SELECT T2.dept_name FROM student AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code ORDER BY stu_gpa LIMIT 1
SELECT T2.dept_store_id ,  T2.store_name FROM departments AS T1 JOIN department_stores AS T2 ON T1.dept_store_id  =  T2.dept_store_id WHERE T1.department_name  =  \"marketing\" INTERSECT SELECT T2.dept_store_id ,  T2.store_name FROM departments AS T1 JOIN department_stores AS T2 ON T1.dept_store_id  =  T2.dept_store_id WHERE T1.department_name  =  \"managing\"
SELECT T2.directed_by FROM schedule AS T1 JOIN film AS T2 ON T1.film_id  =  T2.film_id GROUP BY T2.directed_by ORDER BY sum(T1.show_times_per_day) DESC LIMIT 1
SELECT T2.document_name FROM Templates AS T1 JOIN Documents AS T2 ON T1.template_id  =  T2.template_id WHERE T1.template_type_code  =  \"BK\"
SELECT T2.document_type_name FROM All_documents AS T1 JOIN Ref_document_types AS T2 ON T1.document_type_code  =  T2.document_type_code WHERE T1.document_name  =  \"How to read a book\"
SELECT T2.driverid ,  T3.forename FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid JOIN drivers AS T3 ON T2.driverid  =  T3.driverid WHERE T1.name = \"Australian Grand Prix\" INTERSECT SELECT T2.driverid ,  T3.forename FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid JOIN drivers AS T3 ON T2.driverid  =  T3.driverid WHERE T1.name = \"Chinese Grand Prix\"
SELECT T2.email_address FROM Addresses AS T1 JOIN Teachers AS T2 ON T1.address_id  =  T2.address_id WHERE T1.zip_postcode  =  \"918\"
SELECT T2.emp_fname ,  T1.class_room FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num JOIN professor AS T3 ON T2.emp_num  =  T3.emp_num JOIN department AS T4 ON T4.dept_code  =  T3.dept_code WHERE T4.dept_name  =  'Accounting'
SELECT T2.emp_fname ,  T1.crs_code FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num
SELECT T2.emp_fname ,  T1.prof_office FROM professor AS T1 JOIN employee AS T2 ON T1.emp_num  =  T2.emp_num JOIN department AS T3 ON T1.dept_code  =  T3.dept_code WHERE T3.dept_name  =  'History' AND T1.prof_high_degree != 'Ph.D.'
SELECT T2.emp_fname ,  T1.prof_office FROM professor AS T1 JOIN employee AS T2 ON T1.emp_num  =  T2.emp_num ORDER BY T2.emp_fname
SELECT T2.emp_fname ,  T3.crs_description FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num JOIN course AS T3 ON T1.crs_code  =  T3.crs_code
SELECT T2.emp_fname ,  T4.prof_office ,  T3.crs_description ,  T5.dept_name FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num JOIN course AS T3 ON T1.crs_code  =  T3.crs_code JOIN professor AS T4 ON T2.emp_num  =  T4.emp_num JOIN department AS T5 ON T4.dept_code  =  T5.dept_code
SELECT T2.emp_fname ,  T4.prof_office ,  T3.crs_description FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num JOIN course AS T3 ON T1.crs_code  =  T3.crs_code JOIN professor AS T4 ON T2.emp_num  =  T4.emp_num
SELECT T2.emp_fname FROM CLASS AS T1 JOIN employee AS T2 ON T1.prof_num  =  T2.emp_num GROUP BY T1.prof_num HAVING count(*)  >  1
SELECT T2.f1 FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f2 WHERE T1.name  =  \"Mary\" INTERSECT SELECT T2.f1 FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f2 WHERE T1.name  =  \"Susan\"
SELECT T2.f1 FROM user_profiles AS T1 JOIN follows AS T2 ON T1.uid  =  T2.f2 WHERE T1.name  =  \"Mary\" OR T1.name  =  \"Susan\"
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2001 ORDER BY t3.degrees LIMIT 1
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2002 ORDER BY t3.degrees DESC LIMIT 1
SELECT T2.feature_type_name FROM Other_Available_Features AS T1 JOIN Ref_Feature_Types AS T2 ON T1.feature_type_code  =  T2.feature_type_code WHERE T1.feature_name  =  \"AirCon\"
SELECT T2.first_name ,  T2.last_name ,  T2.actor_id FROM film_actor AS T1 JOIN actor AS T2 ON T1.actor_id  =  T2.actor_id GROUP BY T2.actor_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.first_name ,  T2.last_name FROM Addresses AS T1 JOIN Students AS T2 ON T1.address_id  =  T2.address_id WHERE T1.state_province_county  =  \"Wisconsin\"
SELECT T2.first_name ,  T2.last_name FROM candidates AS T1 JOIN people AS T2 ON T1.candidate_id = T2.person_id
SELECT T2.first_name ,  T2.last_name FROM film_actor AS T1 JOIN actor AS T2 ON T1.actor_id  =  T2.actor_id GROUP BY T2.actor_id HAVING count(*)  >  30
SELECT T2.firstname ,  T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T3.Title  =  \"Badlands\"
SELECT T2.firstname ,  T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T3.Title  =  \"Badlands\" AND T1.StagePosition  =  \"back\"
SELECT T2.firstname ,  T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T3.Title  =  \"Der Kapitan\" AND T1.StagePosition  =  \"back\"
SELECT T2.firstname ,  T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T3.Title  =  \"Le Pop\"
SELECT T2.firstname ,  T2.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom GROUP BY T2.firstname ,  T2.lastname ORDER BY count(*) DESC LIMIT 1
SELECT T2.firstname ,  T2.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.firstname  =  \"EVELINA\" AND T1.lastname  =  \"BROMLEY\"
SELECT T2.fname ,  T2.lname FROM Faculty AS T1 JOIN Student AS T2 ON T1.FacID  =  T2.advisor WHERE T1.fname  =  \"Michael\" AND T1.lname  =  \"Goodrich\"
SELECT T2.friend FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T2.name  =  'Alice' AND T1.gender  =  'female'
SELECT T2.friend FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T2.name  =  'Alice' AND T1.gender  =  'male' AND T1.job  =  'doctor'
SELECT T2.friend FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T1.name  =  'Bob'
SELECT T2.guest_first_name ,  T1.booking_start_date ,  T1.booking_start_date FROM Apartment_Bookings AS T1 JOIN Guests AS T2 ON T1.guest_id  =  T2.guest_id
SELECT T2.guest_first_name ,  T2.guest_last_name FROM Apartment_Bookings AS T1 JOIN Guests AS T2 ON T1.guest_id  =  T2.guest_id WHERE T1.booking_status_code  =  \"Confirmed\"
SELECT T2.id ,  T2.name FROM death AS T1 JOIN ship AS t2 ON T1.caused_by_ship_id  =  T2.id GROUP BY T2.id ORDER BY count(*) DESC LIMIT 1
SELECT T2.investor_id ,  T1.Investor_details FROM INVESTORS AS T1 JOIN TRANSACTIONS AS T2 ON T1.investor_id  =  T2.investor_id GROUP BY T2.investor_id ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.investor_id ,  T1.Investor_details FROM INVESTORS AS T1 JOIN TRANSACTIONS AS T2 ON T1.investor_id  =  T2.investor_id GROUP BY T2.investor_id ORDER BY COUNT(*) DESC LIMIT 3
SELECT T2.investor_id ,  T1.Investor_details FROM INVESTORS AS T1 JOIN TRANSACTIONS AS T2 ON T1.investor_id  =  T2.investor_id WHERE T2.transaction_type_code  =  \"SALE\" GROUP BY T2.investor_id HAVING COUNT(*)  >=  2
SELECT T2.investor_id FROM INVESTORS AS T1 JOIN TRANSACTIONS AS T2 ON T1.investor_id  =  T2.investor_id GROUP BY T2.investor_id HAVING COUNT(*)  >=  2
SELECT T2.invoice_number ,  T2.invoice_date FROM Financial_transactions AS T1 JOIN Invoices AS T2 ON T1.invoice_number  =  T2.invoice_number GROUP BY T1.invoice_number ORDER BY count(*) DESC LIMIT 1
SELECT T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId GROUP BY lastname ORDER BY count(*) DESC LIMIT 1
SELECT T2.lastname FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id WHERE stageposition  =  \"back\" GROUP BY lastname ORDER BY count(*) DESC LIMIT 1
SELECT T2.lastname FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.firstname  =  \"GELL\" AND T1.lastname  =  \"TAMI\"
SELECT T2.lname ,  T2.fname FROM SportsInfo AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID WHERE T1.SportName  =  \"Football\" OR T1.SportName  =  \"Lacrosse\"
SELECT T2.location_name ,  T1.location_code FROM Document_locations AS T1 JOIN Ref_locations AS T2 ON T1.location_code  =  T2.location_code GROUP BY T1.location_code ORDER BY count(*) ASC LIMIT 1
SELECT T2.lot_details FROM INVESTORS AS T1 JOIN LOTS AS T2 ON  T1.investor_id  =  T2.investor_id WHERE T1.Investor_details  =  \"l\"
SELECT T2.lot_id ,  avg(amount_of_transaction) FROM TRANSACTIONS AS T1 JOIN Transactions_Lots AS T2 ON T1.transaction_id  =  T2.transaction_id GROUP BY T2.lot_id
SELECT T2.lot_id ,  avg(amount_of_transaction) FROM TRANSACTIONS AS T1 JOIN Transactions_Lots AS T2 ON T1.transaction_id  =  T2.transaction_id GROUP BY T2.lot_id ORDER BY avg(amount_of_transaction)
SELECT T2.monthly_rental FROM Addresses AS T1 JOIN Student_Addresses AS T2 ON T1.address_id  =  T2.address_id WHERE T1.state_province_county  =  \"Texas\"
SELECT T2.name ,  T1.category_id ,  count(*) FROM film_category AS T1 JOIN category AS T2 ON T1.category_id  =  T2.category_id GROUP BY T1.category_id
SELECT T2.name ,  T2.capacity FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id WHERE T1.year  >=  2014 GROUP BY T2.stadium_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name ,  T2.hometown FROM membership_register_branch AS T1 JOIN member AS T2 ON T1.member_id  =  T2.member_id WHERE T1.register_year  =  2016
SELECT T2.name ,  T2.location FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id WHERE T1.Year  =  2014 INTERSECT SELECT T2.name ,  T2.location FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id WHERE T1.Year  =  2015
SELECT T2.name ,  T2.location FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id GROUP BY T1.track_id HAVING count(*)  =  1
SELECT T2.name ,  T2.open_year FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id WHERE T1.register_year  =  2016 GROUP BY T2.branch_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name ,  T2.salary FROM advisor AS T1 JOIN instructor AS T2 ON T1.i_id  =  T2.id JOIN student AS T3 ON T1.s_id  =  T3.id WHERE T3.dept_name  =  'History'
SELECT T2.name ,  T2.salary FROM advisor AS T1 JOIN instructor AS T2 ON T1.i_id  =  T2.id JOIN student AS T3 ON T1.s_id  =  T3.id WHERE T3.dept_name  =  'Math'
SELECT T2.name ,  T2.year FROM results AS T1 JOIN races AS T2 ON T1.raceid = T2.raceid JOIN drivers AS T3 ON T1.driverid = T3.driverid WHERE T3.forename = \"Lewis\"
SELECT T2.name ,  T3.name FROM accelerator_compatible_browser AS T1 JOIN browser AS T2 ON T1.browser_id  =  T2.id JOIN web_client_accelerator AS T3 ON T1.accelerator_id  =  T3.id ORDER BY T1.compatible_since_year DESC
SELECT T2.name ,  T3.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T2.hours  =  (SELECT min(hours) FROM projects)
SELECT T2.name ,  T3.name FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id JOIN train AS T3 ON T3.train_id  =  T1.train_id
SELECT T2.name ,  T3.name FROM wedding AS T1 JOIN people AS T2 ON T1.male_id  =  T2.people_id JOIN people AS T3 ON T1.female_id  =  T3.people_id WHERE T1.year  >  2014
SELECT T2.name ,  avg(T1.age) FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend GROUP BY T2.name ORDER BY avg(T1.age) DESC LIMIT 1
SELECT T2.name ,  avg(T1.stars) FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID GROUP BY T2.name
SELECT T2.name ,  count(*) FROM Flight AS T1 JOIN Aircraft AS T2 ON T1.aid  =  T2.aid GROUP BY T1.aid
SELECT T2.name ,  count(*) FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id
SELECT T2.name ,  count(*) FROM Likes AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id
SELECT T2.name ,  count(*) FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id GROUP BY T1.stadium_id
SELECT T2.name ,  count(*) FROM exhibition AS T1 JOIN artist AS T2 ON T1.artist_id  =  T2.artist_id GROUP BY T1.artist_id
SELECT T2.name ,  count(*) FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id WHERE T1.register_year  >  2015 GROUP BY T2.branch_id
SELECT T2.name ,  count(*) FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id GROUP BY T1.track_id
SELECT T2.name ,  count(*) FROM singer_in_concert AS T1 JOIN singer AS T2 ON T1.singer_id  =  T2.singer_id GROUP BY T2.singer_id
SELECT T2.name ,  count(*) FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id GROUP BY T1.station_id
SELECT T2.name ,  sum(T1.show_times_per_day) FROM schedule AS T1 JOIN cinema AS T2 ON T1.cinema_id  =  T2.cinema_id GROUP BY T1.cinema_id
SELECT T2.name , T2.country FROM exhibition AS T1 JOIN artist AS T2 ON T1.artist_id  =  T2.artist_id GROUP BY T1.artist_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM Certificate AS T1 JOIN Aircraft AS T2 ON T2.aid  =  T1.aid GROUP BY T1.aid ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM Certificate AS T1 JOIN Aircraft AS T2 ON T2.aid  =  T1.aid WHERE T2.distance  >  5000 GROUP BY T1.aid ORDER BY count(*)  >=  5
SELECT T2.name FROM Flight AS T1 JOIN Aircraft AS T2 ON T1.aid  =  T2.aid GROUP BY T1.aid HAVING count(*)  >= 2
SELECT T2.name FROM Flight AS T1 JOIN Aircraft AS T2 ON T1.aid  =  T2.aid WHERE T1.flno  =  99
SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id HAVING count(*)  >=  3
SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id INTERSECT SELECT T2.name FROM Likes AS T1 JOIN Highschooler AS T2 ON T1.liked_id  =  T2.id
SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id WHERE T2.grade  >  5 GROUP BY T1.student_id HAVING count(*)  >=  2
SELECT T2.name FROM Likes AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id HAVING count(*)  >=  2
SELECT T2.name FROM Likes AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id GROUP BY T1.student_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM Office_locations AS T1 JOIN buildings AS T2 ON T1.building_id  =  T2.id JOIN Companies AS T3 ON T1.company_id  =  T3.id GROUP BY T1.building_id HAVING COUNT(*)  >  1
SELECT T2.name FROM Office_locations AS T1 JOIN buildings AS T2 ON T1.building_id  =  T2.id JOIN Companies AS T3 ON T1.company_id  =  T3.id GROUP BY T1.building_id ORDER BY COUNT(*) DESC LIMIT 1
SELECT T2.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T1.city  =  'new york city'
SELECT T2.name FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID GROUP BY T1.rID HAVING COUNT(*)  >=  3
SELECT T2.name FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID WHERE T1.stars  =  3 INTERSECT SELECT T2.name FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID WHERE T1.stars  =  4
SELECT T2.name FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID WHERE T1.stars  >  3
SELECT T2.name FROM advisor AS T1 JOIN instructor AS T2 ON T1.i_id  =  T2.id JOIN student AS T3 ON T1.s_id  =  T3.id ORDER BY T3.tot_cred DESC LIMIT 1
SELECT T2.name FROM advisor AS T1 JOIN instructor AS T2 ON T1.i_id  =  T2.id JOIN student AS T3 ON T1.s_id  =  T3.id WHERE T3.dept_name  =  'Math' ORDER BY T3.tot_cred
SELECT T2.name FROM appointment AS T1 JOIN physician AS T2 ON T1.Physician  =  T2.EmployeeID
SELECT T2.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T3.name LIKE '%Smith%'
SELECT T2.name FROM assignedto AS T1 JOIN scientists AS T2 ON T1.scientist  =  T2.ssn
SELECT T2.name FROM film AS T1 JOIN LANGUAGE AS T2 ON T1.language_id  =  T2.language_id WHERE T1.title  =  'AIRPORT POLLOCK'
SELECT T2.name FROM game AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.id JOIN injury_accident AS T3 ON T1.id  =  T3.game_id WHERE T3.player  =  'Walter Samuel' INTERSECT SELECT T2.name FROM game AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.id JOIN injury_accident AS T3 ON T1.id  =  T3.game_id WHERE T3.player  =  'Thiago Motta'
SELECT T2.name FROM genres AS T1 JOIN tracks AS T2 ON T1.id = T2.genre_id WHERE T1.name = \"Rock\" OR T1.name = \"Jazz\"
SELECT T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id WHERE T3.Hometown  =  'Louisville ,  Kentucky' INTERSECT SELECT T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id WHERE T3.Hometown  =  'Hiram ,  Georgia'
SELECT T2.name FROM photos AS T1 JOIN camera_lens AS T2 ON T1.camera_lens_id  =  T2.id GROUP BY T2.id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id GROUP BY T1.track_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM singer_in_concert AS T1 JOIN singer AS T2 ON T1.singer_id  =  T2.singer_id JOIN concert AS T3 ON T1.concert_id  =  T3.concert_id WHERE T3.year  =  2014
SELECT T2.name FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id GROUP BY T1.station_id HAVING count(*)  >=  2
SELECT T2.name FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id GROUP BY T1.station_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.name FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id JOIN train AS T3 ON T3.train_id  =  T1.train_id WHERE T3.Name  =  \"Ananthapuri Express\" INTERSECT SELECT T2.name FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id JOIN train AS T3 ON T3.train_id  =  T1.train_id WHERE T3.Name  =  \"Guruvayur Express\"
SELECT T2.name FROM train_station AS T1 JOIN train AS T2 ON T1.train_id  =  T2.train_id WHERE T1.station_id NOT IN (SELECT T4.station_id FROM train_station AS T3 JOIN station AS T4 ON T3.station_id  =  T4.station_id WHERE t4.location  =  \"London\")
SELECT T2.name FROM wedding AS T1 JOIN people AS T2 ON T1.female_id  =  T2.people_id WHERE T1.year  =  2016 AND T2.is_male  =  'F' AND T2.country  =  'Canada'
SELECT T2.name_first ,  T2.name_last FROM salary AS T1 JOIN player AS T2 ON T1.player_id = T2.player_id JOIN team AS T3 ON T3.team_id_br = T1.team_id WHERE T1.year = 2005 AND T3.name = 'Washington Nationals' INTERSECT SELECT T2.name_first , T2.name_last FROM salary AS T1 JOIN player AS T2 ON T1.player_id = T2.player_id JOIN team AS T3 ON T3.team_id_br = T1.team_id WHERE T1.year = 2007 AND T3.name = 'Washington Nationals'
SELECT T2.order_id ,  T2.order_details FROM Invoices AS T1 JOIN Orders AS T2 ON T1.order_id  =  T2.order_id GROUP BY T2.order_id HAVING count(*)  >  2
SELECT T2.order_id ,  T2.order_status FROM customers AS T1 JOIN orders AS T2 ON T1.customer_id = T2.customer_id WHERE T1.customer_name = \"Jeramie\"
SELECT T2.organisation_id ,  T2.organisation_details FROM Grants AS T1 JOIN Organisations AS T2 ON T1.organisation_id  =  T2.organisation_id GROUP BY T2.organisation_id HAVING sum(T1.grant_amount)  >  6000
SELECT T2.pName FROM  tryout AS T1 JOIN player AS T2 ON T1.pID  =  T2.pID WHERE T1.cName  =  (SELECT cName FROM college ORDER BY enr DESC LIMIT 1)
SELECT T2.party_name ,  count(*) FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id GROUP BY T1.party_id
SELECT T2.party_name ,  count(*) FROM party_events AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id GROUP BY T1.party_id
SELECT T2.party_name FROM Member AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id GROUP BY T1.party_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.party_name FROM party_events AS T1 JOIN party AS T2 ON T1.party_id  =  T2.party_id GROUP BY T1.party_id HAVING count(*)  >=  2
SELECT T2.petid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid WHERE T1.Lname  =  'Smith'
SELECT T2.pilot FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id WHERE T1.principal_activities  =  'Cargo' INTERSECT SELECT T2.pilot FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id WHERE T1.principal_activities  =  'Catering services'
SELECT T2.premises_type ,  T1.address_type_code FROM customer_addresses AS T1 JOIN premises AS T2 ON T1.premise_id  =  T2.premise_id
SELECT T2.product_description FROM products_booked AS T1 JOIN products_for_hire AS T2 ON T1.product_id  =  T2.product_id WHERE T1.booked_amount  =  102.76
SELECT T2.product_name ,  count(*) FROM Order_items AS T1 JOIN Products AS T2 ON T1.product_id  =  T2.product_id JOIN Orders AS T3 ON T3.order_id  =  T1.order_id GROUP BY T2.product_name
SELECT T2.product_name ,  sum(T1.product_quantity) FROM Order_items AS T1 JOIN Products AS T2 ON T1.product_id  =  T2.product_id GROUP BY T2.product_name
SELECT T2.product_name FROM problems AS T1 JOIN product AS T2 ON T1.product_id = T2.product_id GROUP BY T2.product_name ORDER BY count(*) DESC LIMIT 3
SELECT T2.product_name FROM view_product_availability AS T1 JOIN products_for_hire AS T2 ON T1.product_id  =  T2.product_id WHERE T1.available_yn  =  1
SELECT T2.product_type_code ,  T2.product_name ,  T2.product_price FROM product_suppliers AS T1 JOIN products AS T2 ON T1.product_id  =  T2.product_id WHERE T1.supplier_id  =  3
SELECT T2.property_type_description FROM Properties AS T1 JOIN Ref_Property_Types AS T2 ON T1.property_type_code  =  T2.property_type_code GROUP BY T1.property_type_code
SELECT T2.protein_name ,  T1.institution FROM institution AS T1 JOIN protein AS T2 ON T1.institution_id  =  T2.institution_id
SELECT T2.region_name FROM affected_region AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id JOIN storm AS T3 ON T1.storm_id  =  T3.storm_id ORDER BY T3.Number_Deaths DESC LIMIT 1
SELECT T2.region_name FROM affected_region AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id JOIN storm AS T3 ON T1.storm_id  =  T3.storm_id WHERE T3.number_deaths  >=  10
SELECT T2.role_name ,  T2.role_description FROM Employees AS T1 JOIN ROLES AS T2 ON T1.role_code  =  T2.role_code WHERE T1.employee_name  =  \"Ebba\"
SELECT T2.roomName ,  count(*) ,  T1.Room FROM Reservations AS T1 JOIN Rooms AS T2 ON T1.Room  =  T2.RoomId GROUP BY T1.Room
SELECT T2.roomName FROM Reservations AS T1 JOIN Rooms AS T2 ON T1.Room  =  T2.RoomId GROUP BY T1.Room HAVING count(*)  >  60
SELECT T2.roomName FROM Reservations AS T1 JOIN Rooms AS T2 ON T1.Room  =  T2.RoomId WHERE firstname LIKE '%ROY%'
SELECT T2.school ,  T3.name FROM school_bus AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id JOIN driver AS T3 ON T1.driver_id  =  T3.driver_id
SELECT T2.school_name ,  T1.budgeted ,  T1.invested FROM budget AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T1.year  >=  2002
SELECT T2.school_name FROM budget AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id JOIN endowment AS T3 ON T2.school_id  =  T3.school_id GROUP BY T2.school_name HAVING sum(T1.budgeted)  >  100 OR sum(T3.amount)  >  10
SELECT T2.school_name FROM endowment AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id GROUP BY T1.school_id HAVING sum(T1.amount)  <=  10
SELECT T2.song_name FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T1.duration LIKE \"4:%\" UNION SELECT song_name FROM song WHERE languages  =  \"english\"
SELECT T2.song_name FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T1.formats  =  \"mp3\" INTERSECT SELECT song_name FROM song WHERE resolution  <  1000
SELECT T2.star_rating_description FROM HOTELS AS T1 JOIN Ref_Hotel_Star_Ratings AS T2 ON T1.star_rating_code  =  T2.star_rating_code WHERE T1.price_range  >  10000
SELECT T2.student_details FROM student_course_registrations AS T1 JOIN students AS T2 ON T1.student_id = T2.student_id ORDER BY T1.registration_date DESC LIMIT 1
SELECT T2.student_id FROM courses AS T1 JOIN student_course_attendance AS T2 ON T1.course_id = T2.course_id WHERE T1.course_name = \"statistics\" ORDER BY T2.date_of_attendance
SELECT T2.student_id FROM courses AS T1 JOIN student_course_registrations AS T2 ON T1.course_id = T2.course_id WHERE T1.course_name = \"statistics\" ORDER BY T2.registration_date
SELECT T2.template_id FROM Ref_template_types AS T1 JOIN Templates AS T2 ON T1.template_type_code  = T2.template_type_code WHERE T1.template_type_description  =  \"Presentation\"
SELECT T2.theme ,  T1.date ,  T1.attendance FROM exhibition_record AS T1 JOIN exhibition AS T2 ON T1.exhibition_id  =  T2.exhibition_id WHERE T2.year  =  2004
SELECT T2.theme FROM exhibition_record AS T1 JOIN exhibition AS T2 ON T1.exhibition_id  =  T2.exhibition_id WHERE T1.attendance  <  100 INTERSECT SELECT T2.theme FROM exhibition_record AS T1 JOIN exhibition AS T2 ON T1.exhibition_id  =  T2.exhibition_id WHERE T1.attendance  >  500
SELECT T2.title ,  T1.mID FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID GROUP BY T1.mID ORDER BY count(*) DESC LIMIT 1
SELECT T2.title ,  T1.rID ,  T1.stars ,  min(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID GROUP BY T1.rID
SELECT T2.title ,  T1.stars ,  T2.director ,  max(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE director != \"null\" GROUP BY director
SELECT T2.title ,  T1.stars ,  T2.director ,  min(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID GROUP BY T2.director
SELECT T2.title ,  T2.director FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T1.stars  >  (SELECT avg(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T2.director  =  \"James Cameron\")
SELECT T2.title ,  T2.film_id ,  T2.description FROM film_actor AS T1 JOIN film AS T2 ON T1.film_id  =  T2.film_id GROUP BY T2.film_id ORDER BY count(*) DESC LIMIT 1
SELECT T2.title ,  T2.year FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID ORDER BY T1.stars DESC LIMIT 3
SELECT T2.title ,  avg(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID GROUP BY T1.mID ORDER BY avg(T1.stars) DESC LIMIT 1
SELECT T2.title ,  avg(T1.stars) FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID GROUP BY T1.mID ORDER BY avg(T1.stars) LIMIT 1
SELECT T2.title ,  max(T1.price) FROM schedule AS T1 JOIN film AS T2 ON T1.film_id  =  T2.film_id GROUP BY T1.film_id
SELECT T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T1.stars  =  3 INTERSECT SELECT T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T1.stars  =  4
SELECT T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T1.stars BETWEEN 3 AND 5
SELECT T2.title FROM artists AS T1 JOIN albums AS T2 ON T1.id  =  T2.artist_id WHERE T1.name LIKE '%Led%'
SELECT T2.transcript_date ,  T1.transcript_id FROM Transcript_Contents AS T1 JOIN Transcripts AS T2 ON T1.transcript_id  =  T2.transcript_id GROUP BY T1.transcript_id HAVING count(*)  >=  2
SELECT T2.transcript_date ,  T1.transcript_id FROM Transcript_Contents AS T1 JOIN Transcripts AS T2 ON T1.transcript_id  =  T2.transcript_id GROUP BY T1.transcript_id ORDER BY count(*) ASC LIMIT 1
SELECT T2.type ,  count(*) FROM school_bus AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id GROUP BY T2.type
SELECT T2.year FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id  =  T2.host_city WHERE T1.city  =  \"Taizhou ( Zhejiang )\"
SELECT T2.zip_postcode FROM Customers AS T1 JOIN Addresses AS T2 ON T1.customer_address_id = T2.address_id WHERE T1.first_name = \"Carole\" AND T1.last_name = \"Bernhard\"
SELECT T3.Date FROM member_attendance AS T1 JOIN member AS T2 ON T1.Member_ID  =  T2.Member_ID JOIN performance AS T3 ON T1.Performance_ID  =  T3.Performance_ID WHERE T2.Role  =  \"Violin\"
SELECT T3.EMP_FNAME ,  T3.EMP_LNAME FROM professor AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code JOIN employee AS T3 ON T1.EMP_NUM  =  T3.EMP_NUM WHERE DEPT_NAME  =  \"Biology\"
SELECT T3.Festival_Name FROM nomination AS T1 JOIN artwork AS T2 ON T1.Artwork_ID  =  T2.Artwork_ID JOIN festival_detail AS T3 ON T1.Festival_ID  =  T3.Festival_ID WHERE T2.Type  =  \"Program Talent Show\"
SELECT T3.Location ,  T2.Name FROM party_host AS T1 JOIN HOST AS T2 ON T1.Host_ID  =  T2.Host_ID JOIN party AS T3 ON T1.Party_ID  =  T3.Party_ID ORDER BY T2.Age
SELECT T3.Location FROM party_host AS T1 JOIN HOST AS T2 ON T1.Host_ID  =  T2.Host_ID JOIN party AS T3 ON T1.Party_ID  =  T3.Party_ID WHERE T2.Age  >  50
SELECT T3.Name ,  COUNT(*) FROM employment AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID JOIN company AS T3 ON T1.Company_ID  =  T3.Company_ID GROUP BY T3.Name
SELECT T3.Name ,  COUNT(*) FROM news_report AS T1 JOIN event AS T2 ON T1.Event_ID  =  T2.Event_ID JOIN journalist AS T3 ON T1.journalist_ID  =  T3.journalist_ID GROUP BY T3.Name
SELECT T3.Name ,  T2.Course FROM course_arrange AS T1 JOIN course AS T2 ON T1.Course_ID  =  T2.Course_ID JOIN teacher AS T3 ON T1.Teacher_ID  =  T3.Teacher_ID
SELECT T3.Name ,  T2.Course FROM course_arrange AS T1 JOIN course AS T2 ON T1.Course_ID  =  T2.Course_ID JOIN teacher AS T3 ON T1.Teacher_ID  =  T3.Teacher_ID ORDER BY T3.Name
SELECT T3.Name ,  T2.Date ,  T2.Venue FROM debate_people AS T1 JOIN debate AS T2 ON T1.Debate_ID  =  T2.Debate_ID JOIN people AS T3 ON T1.Affirmative  =  T3.People_ID
SELECT T3.Name ,  T2.Date ,  T2.Venue FROM debate_people AS T1 JOIN debate AS T2 ON T1.Debate_ID  =  T2.Debate_ID JOIN people AS T3 ON T1.Negative  =  T3.People_ID ORDER BY T3.Name ASC
SELECT T3.Name ,  T2.Date FROM news_report AS T1 JOIN event AS T2 ON T1.Event_ID  =  T2.Event_ID JOIN journalist AS T3 ON T1.journalist_ID  =  T3.journalist_ID
SELECT T3.Name ,  T2.District FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID
SELECT T3.Name ,  T2.District FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID ORDER BY T2.Ranking
SELECT T3.Name ,  T2.Machine_series FROM repair_assignment AS T1 JOIN machine AS T2 ON T1.machine_id  =  T2.machine_id JOIN technician AS T3 ON T1.technician_ID  =  T3.technician_ID
SELECT T3.Name ,  T2.Name FROM employment AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID JOIN company AS T3 ON T1.Company_ID  =  T3.Company_ID
SELECT T3.Name ,  T2.Name FROM employment AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID JOIN company AS T3 ON T1.Company_ID  =  T3.Company_ID ORDER BY T1.Year_working
SELECT T3.Name ,  T2.Name FROM news_report AS T1 JOIN event AS T2 ON T1.Event_ID  =  T2.Event_ID JOIN journalist AS T3 ON T1.journalist_ID  =  T3.journalist_ID ORDER BY T2.Event_Attendance ASC
SELECT T3.Name FROM affected_region AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id JOIN storm AS T3 ON T1.storm_id  =  T3.storm_id WHERE T2.Region_name  =  'Afghanistan' INTERSECT SELECT T3.Name FROM affected_region AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id JOIN storm AS T3 ON T1.storm_id  =  T3.storm_id WHERE T2.Region_name  =  'Albania'
SELECT T3.Name FROM course_arrange AS T1 JOIN course AS T2 ON T1.Course_ID  =  T2.Course_ID JOIN teacher AS T3 ON T1.Teacher_ID  =  T3.Teacher_ID WHERE T2.Course  =  \"Math\"
SELECT T3.Name FROM debate_people AS T1 JOIN debate AS T2 ON T1.Debate_ID  =  T2.Debate_ID JOIN people AS T3 ON T1.Affirmative  =  T3.People_ID WHERE T2.Num_of_Audience  >  200
SELECT T3.Name FROM news_report AS T1 JOIN event AS T2 ON T1.Event_ID  =  T2.Event_ID JOIN journalist AS T3 ON T1.journalist_ID  =  T3.journalist_ID GROUP BY T3.Name HAVING COUNT(*)  >  1
SELECT T3.Name FROM phone_market AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID JOIN phone AS T3 ON T1.Phone_ID  =  T3.Phone_ID WHERE T2.Num_of_shops  >  50
SELECT T3.Name FROM repair_assignment AS T1 JOIN machine AS T2 ON T1.machine_id  =  T2.machine_id JOIN technician AS T3 ON T1.technician_ID  =  T3.technician_ID ORDER BY T2.quality_rank
SELECT T3.Name FROM repair_assignment AS T1 JOIN machine AS T2 ON T1.machine_id  =  T2.machine_id JOIN technician AS T3 ON T1.technician_ID  =  T3.technician_ID WHERE T2.value_points  >  70
SELECT T3.Party_Theme ,  T2.Name FROM party_host AS T1 JOIN HOST AS T2 ON T1.Host_ID  =  T2.Host_ID JOIN party AS T3 ON T1.Party_ID  =  T3.Party_ID
SELECT T3.Pilot_name ,  T2.Fleet_Series FROM pilot_record AS T1 JOIN aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN pilot AS T3 ON T1.Pilot_ID  =  T3.Pilot_ID ORDER BY T3.Rank
SELECT T3.Pilot_name ,  T2.Model FROM pilot_record AS T1 JOIN aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN pilot AS T3 ON T1.Pilot_ID  =  T3.Pilot_ID
SELECT T3.Player_name ,  T2.coach_name FROM player_coach AS T1 JOIN coach AS T2 ON T1.Coach_ID  =  T2.Coach_ID JOIN player AS T3 ON T1.Player_ID  =  T3.Player_ID
SELECT T3.Player_name ,  T2.coach_name FROM player_coach AS T1 JOIN coach AS T2 ON T1.Coach_ID  =  T2.Coach_ID JOIN player AS T3 ON T1.Player_ID  =  T3.Player_ID ORDER BY T3.Votes DESC
SELECT T3.Player_name ,  T3.gender FROM player_coach AS T1 JOIN coach AS T2 ON T1.Coach_ID  =  T2.Coach_ID JOIN player AS T3 ON T1.Player_ID  =  T3.Player_ID WHERE T1.Starting_year  >  2011
SELECT T3.Player_name FROM player_coach AS T1 JOIN coach AS T2 ON T1.Coach_ID  =  T2.Coach_ID JOIN player AS T3 ON T1.Player_ID  =  T3.Player_ID WHERE T2.Rank  =  1
SELECT T3.Shop_Name ,  T2.Carrier FROM stock AS T1 JOIN device AS T2 ON T1.Device_ID  =  T2.Device_ID JOIN shop AS T3 ON T1.Shop_ID  =  T3.Shop_ID
SELECT T3.Title FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T2.Lastname  =  \"Heilo\"
SELECT T3.Title FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId WHERE T2.firstname  =  \"Marianne\"
SELECT T3.activity_name FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID JOIN Activity AS T3 ON T3.actid  =  T2.actid WHERE T1.fname  =  \"Mark\" AND T1.lname  =  \"Giuliano\"
SELECT T3.amenity_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid JOIN lives_in AS T4 ON T4.dormid  =  T1.dormid JOIN student AS T5 ON T5.stuid  =  T4.stuid WHERE T5.lname  =  'Smith'
SELECT T3.amenity_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T1.dorm_name  =  'Smith Hall'
SELECT T3.amenity_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T1.dorm_name  =  'Smith Hall' ORDER BY T3.amenity_name
SELECT T3.area_code FROM contestants AS T1 JOIN votes AS T2 ON T1.contestant_number  =  T2.contestant_number JOIN area_code_state AS T3 ON T2.state  =  T3.state WHERE T1.contestant_name  =  'Tabatha Gehling' INTERSECT SELECT T3.area_code FROM contestants AS T1 JOIN votes AS T2 ON T1.contestant_number  =  T2.contestant_number JOIN area_code_state AS T3 ON T2.state  =  T3.state WHERE T1.contestant_name  =  'Kelly Clauss'
SELECT T3.balance FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid JOIN savings AS T3 ON T1.custid  =  T3.custid ORDER BY T2.balance DESC LIMIT 1
SELECT T3.booking_start_date ,   T3.booking_end_date FROM Products_for_hire AS T1 JOIN products_booked AS T2 ON T1.product_id  =  T2.product_id JOIN bookings AS T3 ON T2.booking_id  =  T3.booking_id WHERE T1.product_name  =  'Book collection A'
SELECT T3.born_state FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T1.name  =  'Treasury' INTERSECT SELECT T3.born_state FROM department AS T1 JOIN management AS T2 ON T1.department_id  =  T2.department_id JOIN head AS T3 ON T2.head_id  =  T3.head_id WHERE T1.name  =  'Homeland Security'
SELECT T3.brand FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id JOIN camera_lens AS T3 ON T2.camera_lens_id  =  T3.id WHERE T1.range  =  'Toubkal Atlas' INTERSECT SELECT T3.brand FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id JOIN camera_lens AS T3 ON T2.camera_lens_id  =  T3.id WHERE T1.range  =  'Lasta Massif'
SELECT T3.cell_mobile_number FROM candidates AS T1 JOIN candidate_assessments AS T2 ON T1.candidate_id = T2.candidate_id JOIN people AS T3 ON T1.candidate_id = T3.person_id WHERE T2.asessment_outcome_code = \"Fail\"
SELECT T3.course_name ,  count(*) FROM students AS T1 JOIN student_course_registrations AS T2 ON T1.student_id = T2.student_id JOIN courses AS T3 ON T2.course_id = T3.course_id GROUP BY T2.course_id
SELECT T3.dept_name  ,  T2.prof_office FROM employee AS T1 JOIN professor AS T2 ON T1.emp_num  =  T2.emp_num JOIN department AS T3 ON T2.dept_code  =  T3.dept_code WHERE T1.emp_lname  =  'Heffington'
SELECT T3.dept_name FROM course AS T1 JOIN CLASS AS T2 ON T1.crs_code  =  T2.crs_code JOIN department AS T3 ON T1.dept_code  =  T3.dept_code GROUP BY T1.dept_code ORDER BY sum(T1.crs_credit) DESC LIMIT 1
SELECT T3.forename ,  T3.surname FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid JOIN drivers AS T3 ON T2.driverid = T3.driverid WHERE T1.name = \"Australian Grand Prix\" EXCEPT SELECT T3.forename ,  T3.surname FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid JOIN drivers AS T3 ON T2.driverid = T3.driverid WHERE T1.name = \"Chinese Grand Prix\"
SELECT T3.id ,  count(*) FROM operate_company AS T1 JOIN flight AS t2 ON T1.id  =  T2.company_id JOIN airport AS T3 ON T2.airport_id  =  T3.id GROUP BY T3.id
SELECT T3.location ,  T3.Representative_Name FROM station_company AS T1 JOIN company AS T2 ON T1.company_id  =  T2.company_id JOIN gas_station AS T3 ON T1.station_id  =  T3.station_id ORDER BY T2.Assets_billion DESC LIMIT 3
SELECT T3.location FROM station_company AS T1 JOIN company AS T2 ON T1.company_id  =  T2.company_id JOIN gas_station AS T3 ON T1.station_id  =  T3.station_id WHERE T2.market_value  >  100
SELECT T3.location_name FROM All_documents AS T1 JOIN Document_locations AS T2 ON T1.document_id  =  T2.document_id JOIN Ref_locations AS T3 ON T2.location_code  =  T3.location_code WHERE T1.document_name  =  \"Robin CV\"
SELECT T3.manager_name FROM station_company AS T1 JOIN company AS T2 ON T1.company_id  =  T2.company_id JOIN gas_station AS T3 ON T1.station_id  =  T3.station_id WHERE T2.company  =  'ExxonMobil'
SELECT T3.name ,  T2.name FROM Office_locations AS T1 JOIN buildings AS T2 ON T1.building_id  =  T2.id JOIN Companies AS T3 ON T1.company_id  =  T3.id
SELECT T3.name ,  T2.name FROM membership_register_branch AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id JOIN member AS T3 ON T1.member_id  =  T3.member_id ORDER BY T1.register_year
SELECT T3.name ,  T2.title ,  T1.date ,  T1.price FROM schedule AS T1 JOIN film AS T2 ON T1.film_id  =  T2.film_id JOIN cinema AS T3 ON T1.cinema_id  =  T3.cinema_id
SELECT T3.name ,  T2.title ,  T1.stars ,  T1.ratingDate FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID JOIN Reviewer AS T3 ON T1.rID  =  T3.rID ORDER BY T3.name ,  T2.title ,  T1.stars
SELECT T3.name ,  T3.time FROM train_station AS T1 JOIN station AS T2 ON T1.station_id  =  T2.station_id JOIN train AS T3 ON T3.train_id  =  T1.train_id WHERE T2.location  =  'London' ORDER BY T3.time DESC
SELECT T3.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T1.name  =  \"John Williams\"
SELECT T3.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id JOIN Highschooler AS T3 ON T1.friend_id  =  T3.id WHERE T2.name  =  \"Kyle\"
SELECT T3.name FROM affected_region AS T1 JOIN region AS T2 ON T1.region_id  =  T2.region_id JOIN storm AS T3 ON T1.storm_id  =  T3.storm_id WHERE T2.region_name  =  'Denmark'
SELECT T3.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T2.name  =  'Matter of Time' INTERSECT SELECT T3.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T2.name  =  'A Puzzling Parallax'
SELECT T3.name FROM course AS T1 JOIN takes AS T2 ON T1.course_id  =  T2.course_id JOIN student AS T3 ON T2.id  =  T3.id WHERE T1.dept_name  =  'Statistics'
SELECT T3.name FROM exhibition_record AS T1 JOIN exhibition AS T2 ON T1.exhibition_id  =  T2.exhibition_id JOIN artist AS T3 ON T3.artist_id  =  T2.artist_id GROUP BY T3.artist_id HAVING avg(T1.attendance)  >  200
SELECT T3.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T1.name  =  \"John Wen\"
SELECT T3.name FROM web_client_accelerator AS T1 JOIN accelerator_compatible_browser AS T2 ON T2.accelerator_id  =  T1.id JOIN browser AS T3 ON T2.browser_id  =  T3.id WHERE T1.name  =  'CACHEbox' INTERSECT SELECT T3.name FROM web_client_accelerator AS T1 JOIN accelerator_compatible_browser AS T2 ON T2.accelerator_id  =  T1.id JOIN browser AS T3 ON T2.browser_id  =  T3.id WHERE T1.name  =  'Fasterfox'
SELECT T3.service_id ,  T4.Service_Type_Code FROM participants AS T1 JOIN Participants_in_Events AS T2 ON T1.Participant_ID  =  T2.Participant_ID JOIN EVENTS AS T3 ON T2.Event_ID   =  T3.Event_ID JOIN services AS T4 ON T3.service_id  =  T4.service_id GROUP BY T3.service_id ORDER BY count(*) ASC LIMIT 1
SELECT T3.ssn ,  T3.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T2.hours  =  (SELECT max(hours) FROM projects)
SELECT T3.stu_fname FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN student AS T3 ON T2.stu_num  =  T3.stu_num WHERE T1.crs_code  =  'ACCT-211'
SELECT T3.stu_fname FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN student AS T3 ON T2.stu_num  =  T3.stu_num WHERE T1.crs_code  =  'ACCT-211' AND T2.enroll_grade  =  'C'
SELECT T3.title ,  T3.credits FROM classroom AS T1 JOIN SECTION AS T2 ON T1.building  =  T2.building AND T1.room_number  =  T2.room_number JOIN course AS T3 ON T2.course_id  =  T3.course_id WHERE T1.capacity  =  (SELECT max(capacity) FROM classroom)
SELECT T3.title FROM albums AS T1 JOIN tracklists AS T2 ON T1.aid  =  T2.albumid JOIN songs AS T3 ON T2.songid  =  T3.songid WHERE T1.title  =  \"A Kiss Before You Go: Live in Hamburg\"
SELECT T3.title FROM albums AS T1 JOIN tracklists AS T2 ON T1.aid  =  T2.albumid JOIN songs AS T3 ON T2.songid  =  T3.songid WHERE t1.label  =  \"Universal Music Group\"
SELECT T3.venue FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id = T2.host_city JOIN MATCH AS T3 ON T2.match_id = T3.match_id WHERE T1.city = \"Nanjing ( Jiangsu )\" AND T3.competition = \"1994 FIFA World Cup qualification\"
SELECT T4.crs_description ,  T4.crs_credit FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN student AS T3 ON T3.stu_num  =  T2.stu_num JOIN course AS T4 ON T4.crs_code  =  T1.crs_code WHERE T3.stu_lname  =  'Smithson'
SELECT T4.dept_name FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN course AS T3 ON T1.crs_code  =  T3.crs_code JOIN department AS T4 ON T3.dept_code  =  T4.dept_code GROUP BY T3.dept_code ORDER BY count(*) DESC LIMIT 1
SELECT T4.instrument FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId JOIN Instruments AS T4 ON T4.songid  =  T3.songid AND T4.bandmateid  =  T2.id WHERE T2.lastname  =  \"Heilo\" AND T3.title  =  \"Badlands\"
SELECT T4.instrument FROM Performance AS T1 JOIN Band AS T2 ON T1.bandmate  =  T2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId JOIN Instruments AS T4 ON T4.songid  =  T3.songid AND T4.bandmateid  =  T2.id WHERE T2.lastname  =  \"Heilo\" AND T3.title  =  \"Le Pop\"
SELECT T4.name FROM stay AS T1 JOIN patient AS T2 ON T1.Patient  =  T2.SSN JOIN Prescribes AS T3 ON T3.Patient  =  T2.SSN JOIN Medication AS T4 ON T3.Medication  =  T4.Code WHERE room  =  111
SELECT T4.name FROM wedding AS T1 JOIN people AS T2 ON T1.male_id  =  T2.people_id JOIN people AS T3 ON T1.female_id  =  T3.people_id JOIN church AS T4 ON T4.church_id  =  T1.church_id WHERE T2.age  >  30 OR T3.age  >  30
SELECT TIME ,  train_number FROM train WHERE destination  =  'Chennai' ORDER BY TIME
SELECT TIME FROM elimination WHERE Eliminated_By  =  \"Punk\" OR Eliminated_By  =  \"Orton\"
SELECT TYPE ,  COUNT(*) FROM artwork GROUP BY TYPE
SELECT TYPE ,  COUNT(*) FROM ship GROUP BY TYPE
SELECT TYPE ,  Nationality FROM ship
SELECT TYPE ,  avg(Tonnage) FROM ship GROUP BY TYPE
SELECT TYPE ,  count(*) ,  sum(enrollment) FROM institution GROUP BY TYPE
SELECT TYPE ,  count(*) FROM mill GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE ,  count(*) FROM operate_company GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE ,  count(*) FROM store GROUP BY TYPE
SELECT TYPE FROM artwork GROUP BY TYPE ORDER BY COUNT(*) DESC LIMIT 1
SELECT TYPE FROM film_market_estimation WHERE YEAR  =  1995
SELECT TYPE FROM institution GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE FROM institution WHERE founded  >  1990 AND enrollment  >=  1000
SELECT TYPE FROM school GROUP BY TYPE HAVING count(*)  =  2
SELECT TYPE FROM ship GROUP BY TYPE ORDER BY COUNT(*) DESC LIMIT 1
SELECT TYPE FROM ship WHERE Tonnage  >  6000 INTERSECT SELECT TYPE FROM ship WHERE Tonnage  <  4000
SELECT TYPE FROM ship WHERE flag  =  'Panama' INTERSECT SELECT TYPE FROM ship WHERE flag  =  'Malta'
SELECT TYPE FROM vocals AS T1 JOIN band AS T2 ON T1.bandmate  =  T2.id WHERE firstname  =  \"Marianne\" GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE FROM vocals AS T1 JOIN band AS T2 ON T1.bandmate  =  T2.id WHERE firstname  =  \"Solveig\" GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE FROM vocals AS T1 JOIN band AS T2 ON T1.bandmate  =  T2.id WHERE lastname  =  \"Heilo\" GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT TYPE FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid JOIN band AS T3 ON T1.bandmate  =  T3.id WHERE T3.firstname  =  \"Solveig\" AND T2.title  =  \"A Bar In Amsterdam\"
SELECT TYPE FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid JOIN band AS T3 ON T1.bandmate  =  T3.id WHERE T3.lastname  =  \"Heilo\" AND T2.title  =  \"Der Kapitan\"
SELECT TYPE FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Badlands\"
SELECT TYPE FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Le Pop\"
SELECT TYPE FROM vocals GROUP BY TYPE ORDER BY count(*) DESC LIMIT 1
SELECT Team ,  COUNT(*) FROM elimination GROUP BY Team
SELECT Team ,  COUNT(*) FROM technician GROUP BY Team
SELECT Team ,  Starting_Year FROM technician
SELECT Team FROM Elimination WHERE Eliminated_By  =  \"Orton\" INTERSECT SELECT Team FROM Elimination WHERE Eliminated_By  =  \"Benjamin\"
SELECT Team FROM elimination GROUP BY Team HAVING COUNT(*)  >  3
SELECT Team FROM player ORDER BY Age DESC LIMIT 1
SELECT Team FROM player ORDER BY Age DESC LIMIT 5
SELECT Team FROM player ORDER BY Team ASC
SELECT Team FROM technician GROUP BY Team HAVING COUNT(*)  >=  2
SELECT Team FROM technician GROUP BY Team ORDER BY COUNT(*) DESC LIMIT 1
SELECT Theme FROM farm_competition ORDER BY YEAR ASC
SELECT Time_of_purchase ,  age ,  address FROM member ORDER BY Time_of_purchase
SELECT Title ,  Issues FROM book
SELECT Title FROM ALBUM AS T1 JOIN ARTIST AS T2 ON T1.ArtistId  =  T2.ArtistId WHERE T2.Name  =  \"AC/DC\"
SELECT Title FROM ALBUM ORDER BY Title
SELECT Title FROM Cartoon ORDER BY title
SELECT Title FROM book ORDER BY Issues ASC
SELECT Title FROM book WHERE Book_ID NOT IN (SELECT Book_ID FROM publication)
SELECT Title FROM book WHERE Writer != \"Elaine Lee\"
SELECT Title FROM film WHERE Film_ID NOT IN (SELECT Film_ID FROM film_market_estimation)
SELECT Total FROM body_builder ORDER BY Total ASC
SELECT Total_Horses FROM farm ORDER BY Total_Horses ASC
SELECT Total_Points FROM gymnast ORDER BY Floor_Exercise_Points DESC
SELECT Total_Points FROM gymnast ORDER BY Total_Points DESC
SELECT Tourist_Details FROM VISITORS
SELECT Venue FROM debate ORDER BY Num_of_Audience ASC
SELECT Votes FROM election ORDER BY Votes DESC
SELECT Weight FROM people ORDER BY Height ASC LIMIT 1
SELECT Wheels ,  LOCATION FROM railway
SELECT Winery FROM WINE GROUP BY Winery HAVING count(*)  >=  4
SELECT Winery FROM WINE ORDER BY SCORE LIMIT 1
SELECT Working_year_starts FROM manager ORDER BY LEVEL DESC
SELECT Writer FROM book GROUP BY Writer HAVING COUNT(*)  >  1
SELECT Writer FROM book ORDER BY Writer ASC
SELECT YEAR ,  book_title ,  publisher FROM book_club ORDER BY YEAR DESC
SELECT YEAR FROM SECTION GROUP BY YEAR ORDER BY count(*) DESC LIMIT 1
SELECT YEAR FROM concert GROUP BY YEAR ORDER BY count(*) DESC LIMIT 1
SELECT YEAR FROM degrees GROUP BY YEAR ORDER BY sum(degrees) DESC LIMIT 1
SELECT YEAR FROM festival_detail GROUP BY YEAR HAVING COUNT(*)  >  1
SELECT YEAR FROM festival_detail WHERE LOCATION  =  'United States' INTERSECT SELECT YEAR FROM festival_detail WHERE LOCATION != 'United States'
SELECT YEAR FROM matches GROUP BY YEAR ORDER BY count(*) DESC LIMIT 1
SELECT Year_of_Founded FROM orchestra AS T1 JOIN performance AS T2 ON T1.Orchestra_ID  =  T2.Orchestra_ID GROUP BY T2.Orchestra_ID HAVING COUNT(*)  >  1
SELECT Zip_code FROM county WHERE County_name  =  \"Howard\"
SELECT acc_percent FROM basketball_match ORDER BY acc_percent DESC LIMIT 1
SELECT account_id ,  account_details FROM Accounts
SELECT account_id ,  count(*) FROM Financial_transactions GROUP BY account_id
SELECT account_id ,  customer_id ,  account_name FROM Accounts
SELECT account_id ,  date_account_opened ,  account_name ,  other_account_details FROM Accounts
SELECT account_id FROM Financial_transactions GROUP BY account_id ORDER BY count(*) DESC LIMIT 1
SELECT activity_name FROM Activity
SELECT actual_order_id FROM actual_orders WHERE order_status_code  =  'Success'
SELECT address ,  num_of_staff FROM shop WHERE shop_id NOT IN (SELECT shop_id FROM happy_hour)
SELECT address FROM member EXCEPT SELECT address FROM member WHERE Membership_card  =  'Black'
SELECT address FROM member WHERE age  <  30 INTERSECT SELECT address FROM member WHERE age  >  40
SELECT address FROM shop ORDER BY open_year
SELECT address_content FROM addresses WHERE city  =  \"East Julianaside\" AND state_province_county  =  \"Texas\" UNION SELECT address_content FROM addresses WHERE city  =  \"Gleasonmouth\" AND state_province_county  =  \"Arizona\"
SELECT address_line_1 ,  address_line_2 FROM customers WHERE email_address  =  \"vbogisich@example.org\"
SELECT address_line_1 FROM Course_Authors_and_Tutors
SELECT address_line_1 FROM Course_Authors_and_Tutors WHERE personal_name         =  \"Cathrine\"
SELECT advisor ,  count(*) FROM Student GROUP BY advisor
SELECT advisor FROM Student GROUP BY advisor HAVING count(*)  >=  2
SELECT advisor FROM Student GROUP BY advisor ORDER BY count(*) DESC LIMIT 1
SELECT age ,  count(*) FROM Student GROUP BY age
SELECT age FROM Person WHERE job  =  'doctor' AND name  =  'Zach'
SELECT aid ,  name FROM Aircraft ORDER BY distance DESC LIMIT 1
SELECT aid FROM Aircraft WHERE distance  >  1000
SELECT allergy ,  allergytype FROM Allergy_type
SELECT allergytype ,  count(*) FROM Allergy_type GROUP BY allergytype
SELECT allergytype FROM Allergy_type GROUP BY allergytype ORDER BY count(*) ASC LIMIT 1
SELECT allergytype FROM Allergy_type GROUP BY allergytype ORDER BY count(*) DESC LIMIT 1
SELECT allergytype FROM Allergy_type WHERE allergy  =  \"Cat\"
SELECT amenity_name FROM dorm_amenity ORDER BY amenity_name
SELECT amount ,  donator_name FROM endowment ORDER BY amount DESC LIMIT 1
SELECT amount FROM payment ORDER BY amount DESC LIMIT 1
SELECT appointmentid FROM appointment ORDER BY START DESC LIMIT 1
SELECT apt_number ,  room_count FROM Apartments
SELECT apt_number FROM Apartments ORDER BY bedroom_count DESC LIMIT 1
SELECT apt_number FROM Apartments ORDER BY room_count ASC
SELECT apt_number FROM Apartments WHERE apt_type_code  =  \"Flat\"
SELECT apt_type_code ,  COUNT(*) FROM Apartments GROUP BY apt_type_code ORDER BY COUNT(*) ASC
SELECT apt_type_code ,  bathroom_count ,  bedroom_count FROM Apartments GROUP BY apt_type_code ORDER BY sum(room_count) DESC LIMIT 1
SELECT apt_type_code ,  max(room_count) ,  min(room_count) FROM Apartments GROUP BY apt_type_code
SELECT apt_type_code FROM Apartments GROUP BY apt_type_code ORDER BY avg(room_count) DESC LIMIT 3
SELECT apt_type_code FROM Apartments GROUP BY apt_type_code ORDER BY count(*) DESC LIMIT 1
SELECT apt_type_code FROM Apartments WHERE bathroom_count  >  1 GROUP BY apt_type_code ORDER BY count(*) DESC LIMIT 1
SELECT artist_name FROM artist WHERE country  =  \"UK\" AND gender  =  \"Male\"
SELECT artist_name FROM artist WHERE country  =  \"UK\" INTERSECT SELECT T1.artist_name FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T2.languages  =  \"english\"
SELECT artist_name FROM artist WHERE country  =  \"UK\" INTERSECT SELECT artist_name FROM song WHERE languages  =  \"english\"
SELECT artist_name FROM song WHERE resolution  >  500 GROUP BY languages ORDER BY count(*) DESC LIMIT 1
SELECT asset_acquired_date FROM Assets ORDER BY asset_acquired_date ASC LIMIT 1
SELECT asset_id ,  asset_details ,  asset_make ,  asset_model FROM Assets
SELECT asset_make ,  asset_model ,  asset_details FROM Assets ORDER BY asset_disposed_date ASC
SELECT asset_model FROM Assets WHERE asset_id NOT IN (SELECT asset_id FROM Fault_Log)
SELECT attribute_data_type FROM Attribute_Definitions GROUP BY attribute_data_type HAVING count(*)  >  3
SELECT attribute_data_type FROM Attribute_Definitions WHERE attribute_name  =  \"Green\"
SELECT average_attendance FROM stadium WHERE capacity_percentage  >  100
SELECT avg(Age) FROM STUDENT WHERE Sex  =  \"F\"
SELECT avg(Age) FROM artist
SELECT avg(Age) FROM manager
SELECT avg(Attendance) FROM SHOW
SELECT avg(Attendance) FROM performance
SELECT avg(Earnings) FROM poker_player
SELECT avg(Enrollment) FROM school
SELECT avg(Event_Attendance) ,  max(Event_Attendance) FROM event
SELECT avg(GNP) ,  sum(population) FROM country WHERE GovernmentForm  =  \"US Territory\"
SELECT avg(Gold) FROM club_rank
SELECT avg(Gross_in_dollar) FROM film
SELECT avg(HS) ,  max(HS) ,  min(HS) FROM Player
SELECT avg(HS) FROM Player
SELECT avg(Horizontal_Bar_Points) FROM gymnast
SELECT avg(Injured) FROM perpetrator
SELECT avg(International_Passengers) FROM airport
SELECT avg(LifeExpectancy) FROM country WHERE Continent  =  \"Africa\" AND GovernmentForm  =  \"Republic\"
SELECT avg(LifeExpectancy) FROM country WHERE Name NOT IN (SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\" AND T2.IsOfficial  =  \"T\")
SELECT avg(LifeExpectancy) FROM country WHERE Region  =  \"Central Africa\"
SELECT avg(Money_Requested) FROM entrepreneur
SELECT avg(Num_of_Audience) FROM festival_detail
SELECT avg(Num_of_Factories) FROM manufacturer WHERE num_of_shops  >  20
SELECT avg(Number_of_hosts) FROM party
SELECT avg(Order_Quantity) FROM Invoices WHERE payment_method_code  =  \"MasterCard\"
SELECT avg(Population) FROM county
SELECT avg(Price) ,  YEAR FROM WINE GROUP BY YEAR
SELECT avg(Price) ,  avg(Score) ,  Appelation FROM WINE GROUP BY Appelation
SELECT avg(Product_Price) FROM Products
SELECT avg(Profits_billion) FROM Companies
SELECT avg(Quantity) FROM stock
SELECT avg(RAM_MiB) FROM chip_model WHERE model_name NOT IN (SELECT chip_model FROM phone)
SELECT avg(Scores) FROM submission
SELECT avg(Snatch) FROM body_builder
SELECT avg(Speed) FROM roller_coaster
SELECT avg(T1.Age) FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  SECRETARY_Vote WHERE T1.Sex  =  \"F\" AND T2.Election_Cycle  =  \"Spring\"
SELECT avg(T1.Age) FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  SECRETARY_Vote WHERE T1.city_code  =  \"NYC\" AND T2.Election_Cycle  =  \"Spring\"
SELECT avg(T1.HS) ,  max(T1.HS) FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T2.decision  =  'yes'
SELECT avg(T1.HS) FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T2.decision  =  'no'
SELECT avg(T1.Price) ,  T2.name FROM products AS T1 JOIN Manufacturers AS T2 ON T1.manufacturer  =  T2.code GROUP BY T2.name
SELECT avg(T1.Price) ,  T2.name FROM products AS T1 JOIN Manufacturers AS T2 ON T1.manufacturer  =  T2.code GROUP BY T2.name HAVING avg(T1.price) >= 150
SELECT avg(T1.Total) FROM body_builder AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T2.Height  >  200
SELECT avg(T1.Votes) FROM election AS T1 JOIN representative AS T2 ON T1.Representative_ID  =  T2.Representative_ID WHERE T2.Party  =  \"Republican\"
SELECT avg(T1.age) ,  T3.dorm_name FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid GROUP BY T3.dorm_name
SELECT avg(T1.age) FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid WHERE T3.student_capacity  =  (SELECT max(student_capacity) FROM dorm)
SELECT avg(T1.duration) FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T1.formats  =  \"mp3\" AND T2.resolution   <  800
SELECT avg(T1.lat) ,  avg(T1.long) FROM station AS T1 JOIN trip AS T2 ON T1.id  =  T2.start_station_id
SELECT avg(T1.price) ,  T2.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code GROUP BY T2.name
SELECT avg(T1.salary) ,  count(*) FROM instructor AS T1 JOIN department AS T2 ON T1.dept_name  =  T2.dept_name ORDER BY T2.budget DESC LIMIT 1
SELECT avg(T1.salary) FROM salary AS T1 JOIN team AS T2 ON T1.team_id  =  T2.team_id_br WHERE T2.name  = 'Boston Red Stockings'
SELECT avg(T1.sales) FROM journal AS T1 JOIN journal_committee AS T2 ON T1.journal_ID  =  T2.journal_ID WHERE T2.work_type  =  'Photo'
SELECT avg(T1.stars) ,  T2.title FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T2.year  =  (SELECT min(YEAR) FROM Movie)
SELECT avg(T2.Age) FROM gymnast AS T1 JOIN people AS T2 ON T1.Gymnast_ID  =  T2.People_ID
SELECT avg(T2.Earnings) FROM people AS T1 JOIN poker_player AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Height  >  200
SELECT avg(T2.Number_cities) FROM film_market_estimation AS T1 JOIN market AS T2 ON T1.Market_ID  =  T2.Market_ID WHERE T1.Low_Estimate  >  10000
SELECT avg(T2.Points) FROM club AS T1 JOIN player AS T2 ON T1.Club_ID  =  T2.Club_ID WHERE T1.name  =  \"AIB\"
SELECT avg(T2.Weeks_on_Top) FROM artist AS T1 JOIN volume AS T2 ON T1.Artist_ID  =  T2.Artist_ID WHERE T1.age  <=  25
SELECT avg(T2.fastestlapspeed) ,  T1.name ,  T1.year FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid WHERE T1.year > 2014 GROUP BY T1.name ORDER BY T1.year
SELECT avg(T2.fastestlapspeed) FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid WHERE T1.year = 2008 AND T1.name = \"Monaco Grand Prix\"
SELECT avg(T2.rating) FROM artist AS T1 JOIN song AS T2 ON T1.artist_name  =  T2.artist_name WHERE T1.gender  =  \"Female\"
SELECT avg(T2.stu_gpa) FROM enroll AS T1 JOIN student AS T2 ON T1.stu_num  =  T2.stu_num JOIN CLASS AS T3 ON T1.class_code  =  T3.class_code WHERE T3.crs_code  =  'ACCT-211'
SELECT avg(T3.Total_Passengers) FROM aircraft AS T1 JOIN airport_aircraft AS T2 ON T1.Aircraft_ID  =  T2.Aircraft_ID JOIN airport AS T3 ON T2.Airport_ID  =  T3.Airport_ID WHERE T1.Aircraft  =  \"Robinson R-22\"
SELECT avg(T3.cost) FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T1.name  =  \"John Wen\"
SELECT avg(Working_Horses) FROM farm WHERE Total_Horses  >  5000
SELECT avg(Years_working) FROM journalist
SELECT avg(acc_bal) ,  acc_type FROM customer WHERE credit_score  <  50 GROUP BY acc_type
SELECT avg(access_count) FROM documents
SELECT avg(access_count) FROM documents GROUP BY document_structure_code ORDER BY count(*) ASC LIMIT 1
SELECT avg(active_to_date - active_from_date) FROM customer_contact_channels
SELECT avg(age) ,  city_code FROM student GROUP BY city_code
SELECT avg(age) ,  gender FROM Person GROUP BY gender
SELECT avg(age) ,  job FROM Person GROUP BY job
SELECT avg(age) ,  job FROM Person WHERE gender  =  'male' GROUP BY job
SELECT avg(age) ,  max(age) ,  sex FROM student GROUP BY sex
SELECT avg(age) ,  min(age) ,  CLASS FROM captain GROUP BY CLASS
SELECT avg(age) ,  min(age) ,  max(age) FROM singer WHERE country  =  'France'
SELECT avg(age) ,  min(age) FROM artist WHERE country  =  'United States'
SELECT avg(age) ,  sex FROM Student GROUP BY sex
SELECT avg(age) FROM Dogs
SELECT avg(age) FROM Dogs WHERE dog_id IN ( SELECT dog_id FROM Treatments )
SELECT avg(age) FROM Person
SELECT avg(age) FROM Student WHERE StuID IN ( SELECT T1.StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"food\" INTERSECT SELECT T1.StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"animal\")
SELECT avg(age) FROM visitor WHERE Level_of_membership  <=  4
SELECT avg(amount_due) FROM payments
SELECT avg(amount_of_transaction) FROM TRANSACTIONS WHERE transaction_type_code  =  \"SALE\"
SELECT avg(balance) FROM checking
SELECT avg(bedroom_count) FROM Apartments
SELECT avg(bikes_available) FROM status WHERE station_id NOT IN (SELECT id FROM station WHERE city  =  \"Palo Alto\")
SELECT avg(budget_million) ,  max(budget_million) ,  min(budget_million) FROM movie WHERE YEAR  <  2000
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  1996
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  2005
SELECT avg(capacity) ,  min(capacity) ,  max(capacity) FROM cinema WHERE openning_year  >=  2011
SELECT avg(capacity) FROM stadium WHERE opening_year  =  2005
SELECT avg(credit_score) FROM customer WHERE cust_id IN (SELECT cust_id FROM loan)
SELECT avg(credit_score) FROM customer WHERE cust_id NOT IN (SELECT cust_id FROM loan)
SELECT avg(damage_millions_USD) ,  max(damage_millions_USD) FROM storm WHERE max_speed  >  1000
SELECT avg(distance) ,  avg(price) FROM Flight WHERE origin  =  \"Los Angeles\"
SELECT avg(elevation) ,  country FROM airports GROUP BY country
SELECT avg(enr) FROM College
SELECT avg(enr) FROM College WHERE state  =  'FL'
SELECT avg(enrollment) ,  max(enrollment) ,  min(enrollment) FROM school
SELECT avg(enrollment) FROM university WHERE founded  <  1850
SELECT avg(floors) ,  max(floors) ,  min(floors) FROM building
SELECT avg(followers) FROM user_profiles WHERE UID IN (SELECT UID FROM tweets)
SELECT avg(followers) FROM user_profiles WHERE UID NOT IN (SELECT UID FROM tweets)
SELECT avg(grade) FROM Highschooler WHERE id IN (SELECT T1.student_id FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id)
SELECT avg(height) ,  avg(weight) FROM people WHERE sex  =  'M'
SELECT avg(high_temperature) ,  day_of_week FROM weekly_weather GROUP BY day_of_week
SELECT avg(hours) FROM projects
SELECT avg(injured) FROM death
SELECT avg(lat) ,  avg(long) FROM station WHERE city  =  \"San Jose\"
SELECT avg(length_feet) FROM bridge
SELECT avg(long) FROM station WHERE id NOT IN (SELECT station_id FROM status GROUP BY station_id HAVING max(bikes_available)  >  10)
SELECT avg(loser_age) ,  avg(winner_age) FROM matches
SELECT avg(no_of_customers) FROM bank
SELECT avg(no_of_customers) FROM bank WHERE state  =  'Utah'
SELECT avg(num_employees) FROM department WHERE ranking BETWEEN 10 AND 15
SELECT avg(num_of_staff) ,  avg(score) FROM shop
SELECT avg(num_of_staff) FROM museum WHERE open_year  <  2009
SELECT avg(num_of_ticket) ,  max(num_of_ticket) FROM visit
SELECT avg(order_quantity) FROM order_items
SELECT avg(pages_per_minute_color) FROM product
SELECT avg(pet_age) ,  max(pet_age) ,  pettype FROM pets GROUP BY pettype
SELECT avg(price) FROM Flight WHERE origin  =  \"Los Angeles\" AND destination  =  \"Honolulu\"
SELECT avg(price) FROM products
SELECT avg(price) FROM products WHERE Manufacturer  =  2
SELECT avg(price) FROM wine WHERE Appelation NOT IN (SELECT T1.Appelation FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T1.County  =  'Sonoma')
SELECT avg(price_in_euros) ,  min(price_in_euros) FROM catalog_contents
SELECT avg(price_range) FROM HOTELS WHERE star_rating_code  =  \"5\" AND pets_allowed_yn  =  1
SELECT avg(product_price) ,  product_category_code FROM products GROUP BY product_category_code
SELECT avg(product_price) FROM products WHERE product_type_code  =  'Clothes'
SELECT avg(prominence) FROM mountain WHERE country  =  'Morocco'
SELECT avg(ranking) ,  T1.first_name FROM players AS T1 JOIN rankings AS T2 ON T1.player_id  =  T2.player_id GROUP BY T1.first_name
SELECT avg(rating) ,  avg(resolution) FROM song WHERE languages  =  \"bangla\"
SELECT avg(rating) ,  languages FROM song GROUP BY languages
SELECT avg(rating) ,  max(rating) FROM review
SELECT avg(revenue) ,  max(revenue) ,  sum(revenue) FROM manufacturers
SELECT avg(room_count) FROM Apartment_Bookings AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T1.booking_status_code  =  \"Provisional\"
SELECT avg(room_count) FROM Apartments WHERE apt_type_code  =  \"Studio\"
SELECT avg(salary) ,  max(salary) FROM Employee
SELECT avg(student_capacity) ,  sum(student_capacity) FROM dorm WHERE gender  =  'X'
SELECT avg(t1.access_count) FROM documents AS t1 JOIN document_functional_areas AS t2 ON t1.document_code  =  t2.document_code JOIN functional_areas AS t3 ON t2.functional_area_code  =  t3.functional_area_code WHERE t3.functional_area_description  =  \"Acknowledgement\"
SELECT avg(t1.age) ,  avg(Years_working) ,  t2.work_type FROM journalist AS t1 JOIN news_report AS t2 ON t1.journalist_id  =  t2.journalist_id GROUP BY t2.work_type
SELECT avg(t3.age) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\"
SELECT avg(t3.age) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Hopkins Student Enterprises\"
SELECT avg(t3.age) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Tennis Club\"
SELECT avg(ticket_price) ,  min(ticket_price) ,  max(ticket_price) FROM exhibition WHERE YEAR  <  2009
SELECT avg(total_amount_purchased) ,  avg(total_value_purchased) FROM Product_Suppliers WHERE supplier_id  =  (SELECT supplier_id FROM Product_Suppliers GROUP BY supplier_id ORDER BY count(*) DESC LIMIT 1)
SELECT avg(total_passengers) ,  max(total_passengers) FROM station WHERE LOCATION  =  'London' OR LOCATION  =  'Glasgow'
SELECT avg(transaction_amount) ,  min(transaction_amount) ,  max(transaction_amount) ,   sum(transaction_amount) FROM Financial_transactions
SELECT avg(transaction_amount) ,  sum(transaction_amount) FROM Financial_transactions
SELECT avg(transcript_date) FROM Transcripts
SELECT avg(velocity) FROM flight WHERE pilot  =  'Thompson'
SELECT avg(weight) ,  min(weight) ,  sex FROM people GROUP BY sex
SELECT avg(weight) ,  pettype FROM pets GROUP BY pettype
SELECT avg(weight) FROM Player
SELECT avg(winner_rank) FROM matches
SELECT bike_id FROM trip WHERE zip_code  =  94002 GROUP BY bike_id ORDER BY COUNT(*) DESC LIMIT 1
SELECT bname FROM bank ORDER BY no_of_customers DESC LIMIT 1
SELECT bname FROM bank WHERE state  =  'New York'
SELECT book_title ,  author_or_editor FROM book_club WHERE YEAR  >  1989
SELECT booking_start_date ,  booking_end_date FROM Apartment_Bookings
SELECT born_state FROM head GROUP BY born_state HAVING count(*)  >=  3
SELECT brand ,  name FROM camera_lens ORDER BY max_aperture DESC
SELECT budget_type_code ,  budget_type_description FROM Ref_budget_codes
SELECT budget_type_code ,  count(*) FROM Documents_with_expenses GROUP BY budget_type_code
SELECT budget_type_code FROM Documents_with_expenses GROUP BY budget_type_code ORDER BY count(*) DESC LIMIT 1
SELECT budget_type_description FROM Ref_budget_codes WHERE budget_type_code  =  \"ORG\"
SELECT building ,  count(*) FROM Faculty GROUP BY building
SELECT building ,  room_number FROM classroom WHERE capacity BETWEEN 50 AND 100
SELECT building FROM Faculty GROUP BY building ORDER BY count(*) DESC LIMIT 1
SELECT building FROM Faculty WHERE rank  =  \"Professor\" GROUP BY building HAVING count(*)  >=  10
SELECT building_address ,  building_phone FROM Apartment_Buildings WHERE building_manager        =  \"Brenden\"
SELECT building_full_name FROM Apartment_Buildings WHERE building_full_name LIKE \"%court%\"
SELECT building_short_name FROM Apartment_Buildings WHERE building_manager       =  \"Emma\"
SELECT built_year FROM ship GROUP BY built_year ORDER BY count(*) DESC LIMIT 1
SELECT cName ,  enr FROM College WHERE enr  >  10000 AND state = \"LA\"
SELECT cName ,  state ,  min(enr) FROM college GROUP BY state
SELECT cName FROM  tryout WHERE decision  =  'yes' AND pPos  =  'goalie'
SELECT cName FROM  tryout WHERE pPos  =  'goalie' INTERSECT SELECT cName FROM  tryout WHERE pPos  =  'mid'
SELECT cName FROM  tryout WHERE pPos  =  'mid' EXCEPT SELECT cName FROM  tryout WHERE pPos  =  'goalie'
SELECT cName FROM College WHERE enr  <  13000 AND state = \"AZ\" UNION SELECT cName FROM College WHERE enr  >  15000 AND state = \"LA\"
SELECT cName FROM College WHERE enr  >  18000 ORDER BY cName
SELECT cName FROM college ORDER BY enr DESC LIMIT 3
SELECT cName FROM college WHERE enr  >  (SELECT max(enr) FROM college WHERE state  =  'FL')
SELECT calendar_date ,  day_Number FROM Ref_calendar
SELECT campus FROM campuses WHERE LOCATION  =  \"Chico\"
SELECT campus FROM campuses WHERE LOCATION  =  \"Northridge\" AND county  =  \"Los Angeles\" UNION SELECT campus FROM campuses WHERE LOCATION  =  \"San Francisco\" AND county  =  \"San Francisco\"
SELECT campus FROM campuses WHERE YEAR  <  1800
SELECT campus FROM campuses WHERE YEAR  =  1958
SELECT campus FROM campuses WHERE YEAR  >=  1935 AND YEAR  <=  1939
SELECT campus FROM campuses WHERE county  =  \"Los Angeles\"
SELECT campus FROM campuses WHERE county  =  \"Los Angeles\" AND YEAR  >  1950
SELECT campus FROM degrees GROUP BY campus ORDER BY sum(degrees) DESC LIMIT 1
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  \"San Francisco State University\" AND T2.year  =  1996
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  \"San Jose State University\" AND T2.year  =  1996
SELECT candidate_id FROM candidate_assessments ORDER BY assessment_date DESC LIMIT 1
SELECT candidate_id FROM candidate_assessments WHERE asessment_outcome_code = \"Pass\"
SELECT card_id ,  customer_id ,  card_type_code ,  card_number FROM Customers_cards
SELECT card_number ,  name ,  hometown FROM member ORDER BY LEVEL DESC
SELECT card_number FROM member WHERE Hometown LIKE \"%Kentucky%\"
SELECT card_type_code ,  count(*) FROM Customers_cards GROUP BY card_type_code
SELECT card_type_code ,  count(DISTINCT customer_id) FROM Customers_cards GROUP BY card_type_code
SELECT card_type_code FROM Customers_cards GROUP BY card_type_code HAVING count(*)  >=  5
SELECT card_type_code FROM Customers_cards GROUP BY card_type_code ORDER BY count(*) DESC LIMIT 1
SELECT catalog_entry_name ,  capacity FROM Catalog_Contents WHERE price_in_dollars  >  700
SELECT catalog_entry_name FROM catalog_contents ORDER BY capacity ASC LIMIT 1
SELECT catalog_entry_name FROM catalog_contents ORDER BY height DESC LIMIT 1
SELECT catalog_entry_name FROM catalog_contents ORDER BY price_in_dollars DESC LIMIT 1
SELECT catalog_entry_name FROM catalog_contents WHERE LENGTH  <  3 OR width  >  5
SELECT catalog_entry_name FROM catalog_contents WHERE next_entry_id  >  8
SELECT catalog_entry_name FROM catalog_contents WHERE product_stock_number LIKE \"2%\"
SELECT catalog_level_name ,  catalog_level_number FROM Catalog_Structure WHERE catalog_level_number BETWEEN 5 AND 10
SELECT catalog_publisher FROM catalogs GROUP BY catalog_publisher ORDER BY count(*) DESC LIMIT 1
SELECT category ,  count(*) FROM book_club GROUP BY category
SELECT category FROM book_club WHERE YEAR  >  1989 GROUP BY category HAVING count(*)  >=  2
SELECT cell_mobile_number ,  email_address FROM STUDENTS
SELECT cell_mobile_number FROM Students WHERE first_name  =  'Timmothy' AND last_name  =  'Ward'
SELECT channel_code ,  contact_number FROM customer_contact_channels WHERE active_to_date - active_from_date  =  (SELECT active_to_date - active_from_date FROM customer_contact_channels ORDER BY (active_to_date - active_from_date) DESC LIMIT 1)
SELECT channel_code FROM customer_contact_channels GROUP BY channel_code HAVING count(customer_id)  <  5
SELECT characteristic_name ,  other_characteristic_details ,  characteristic_data_type FROM CHARACTERISTICS EXCEPT SELECT t1.characteristic_name ,  t1.other_characteristic_details ,  t1.characteristic_data_type FROM CHARACTERISTICS AS t1 JOIN product_characteristics AS t2 ON t1.characteristic_id  =  t2.characteristic_id
SELECT charge_type ,  charge_amount FROM Charges
SELECT circuitid ,  LOCATION FROM circuits WHERE country = \"France\" OR country = \"Belgium\"
SELECT city ,  COUNT(*) FROM station GROUP BY city HAVING COUNT(*)  >=  15
SELECT city ,  GDP FROM city ORDER BY GDP LIMIT 1
SELECT city ,  count(*) FROM airports GROUP BY city HAVING count(*)  >  1
SELECT city ,  count(*) FROM branch WHERE open_year  <  2010 GROUP BY city
SELECT city ,  max(lat) FROM station GROUP BY city
SELECT city ,  state FROM bank WHERE bname  =  'morningside'
SELECT city FROM Addresses ORDER BY city
SELECT city FROM addresses WHERE city NOT IN ( SELECT DISTINCT t3.city FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id)
SELECT city FROM addresses WHERE zip_postcode  =  255
SELECT city FROM airports GROUP BY city HAVING count(*)  =  2
SELECT city FROM airports GROUP BY city HAVING count(*)  >  2 ORDER BY count(*)
SELECT city FROM airports WHERE country  =  'United States' GROUP BY city HAVING count(*)  >  3
SELECT city FROM branch EXCEPT SELECT city FROM branch WHERE membership_amount  >  100
SELECT city FROM branch WHERE open_year  =  2001 AND membership_amount  >  100
SELECT city FROM city ORDER BY regional_population DESC LIMIT 3
SELECT city FROM city WHERE regional_population  >  10000000
SELECT city FROM city WHERE regional_population  >  10000000 UNION SELECT city FROM city WHERE regional_population  <  5000000
SELECT city FROM employee WHERE age  <  30 GROUP BY city HAVING count(*)  >  1
SELECT city FROM station GROUP BY city ORDER BY max(lat) DESC
SELECT city_code ,  count(*) FROM Student GROUP BY city_code
SELECT city_code FROM STUDENT GROUP BY city_code ORDER BY count(*) DESC LIMIT 1
SELECT city_code FROM Student WHERE LName  =  \"Kim\"
SELECT city_code FROM student GROUP BY city_code ORDER BY count(*) DESC LIMIT 1
SELECT claim_status_description FROM claims_processing_stages WHERE claim_status_name  =  \"Open\"
SELECT class_code FROM CLASS WHERE class_room  =  'KLR209'
SELECT classroom ,  count(*) FROM list GROUP BY classroom
SELECT classroom ,  count(*) FROM list WHERE grade  =  \"0\" GROUP BY classroom
SELECT classroom ,  count(*) FROM list WHERE grade  =  \"4\" GROUP BY classroom
SELECT classroom ,  count(DISTINCT grade) FROM list GROUP BY classroom
SELECT classroom FROM list GROUP BY classroom ORDER BY count(*) DESC LIMIT 1
SELECT cloud_cover FROM weather WHERE zip_code  =  94107 GROUP BY cloud_cover ORDER BY COUNT (*) DESC LIMIT 3
SELECT clubdesc FROM club WHERE clubname  =  \"Pen and Paper Gaming\"
SELECT clubdesc FROM club WHERE clubname  =  \"Tennis Club\"
SELECT clublocation FROM club WHERE clubname  =  \"Hopkins Student Enterprises\"
SELECT clublocation FROM club WHERE clubname  =  \"Pen and Paper Gaming\"
SELECT clublocation FROM club WHERE clubname  =  \"Tennis Club\"
SELECT clubname FROM club
SELECT clubname FROM club WHERE clublocation  =  \"AKW\"
SELECT cmi_cross_ref_id FROM cmi_cross_references EXCEPT SELECT cmi_cross_ref_id FROM parking_fines
SELECT code ,  name ,  min(price) FROM products GROUP BY name
SELECT color_description FROM ref_colors
SELECT company ,  headquarters FROM company ORDER BY market_value DESC
SELECT company ,  main_industry FROM company WHERE company_id NOT IN (SELECT company_id FROM station_company)
SELECT company ,  main_industry FROM company WHERE headquarters != 'USA'
SELECT company ,  rank FROM company ORDER BY Sales_billion DESC
SELECT complaint_status_code ,  count(*) FROM complaints WHERE complaint_type_code  =  \"Product Failure\" GROUP BY complaint_status_code
SELECT complaint_status_code FROM complaints GROUP BY complaint_status_code HAVING count(*)  >  3
SELECT contestant_name FROM contestants WHERE contestant_name != 'Jessie Alloway'
SELECT contestant_number ,  contestant_name FROM contestants ORDER BY contestant_name DESC
SELECT cost_of_treatment FROM Treatments ORDER BY date_of_treatment DESC LIMIT 1
SELECT council_tax_id ,  cmi_cross_ref_id FROM parking_fines
SELECT count(*) ,   max(Percentage) FROM countrylanguage WHERE LANGUAGE  =  \"Spanish\" GROUP BY CountryCode
SELECT count(*) ,  Competition FROM MATCH GROUP BY Competition
SELECT count(*) ,  Directed_by FROM cartoon GROUP BY Directed_by
SELECT count(*) ,  District FROM city WHERE Population  >  (SELECT avg(Population) FROM city) GROUP BY District
SELECT count(*) ,  Grape FROM WINE GROUP BY Grape
SELECT count(*) ,  LOCATION FROM shop GROUP BY LOCATION
SELECT count(*) ,  POSITION FROM player WHERE points  <  30 GROUP BY POSITION
SELECT count(*) ,  T1.blockcode FROM BLOCK AS T1 JOIN room AS T2 ON T1.blockfloor  =  T2.blockfloor AND T1.blockcode  =  T2.blockcode GROUP BY T1.blockcode
SELECT count(*) ,  T1.blockfloor FROM BLOCK AS T1 JOIN room AS T2 ON T1.blockfloor  =  T2.blockfloor AND T1.blockcode  =  T2.blockcode GROUP BY T1.blockfloor
SELECT count(*) ,  T1.claim_id FROM Claims AS T1 JOIN Settlements AS T2 ON T1.claim_id  =  T2.claim_id GROUP BY T1.claim_id ORDER BY T1.Date_Claim_Settled DESC LIMIT 1
SELECT count(*) ,  T1.company_id FROM Third_Party_Companies AS T1 JOIN Assets AS T2 ON T1.company_id  =  T2.supplier_company_id GROUP BY T1.company_id
SELECT count(*) ,  T1.director FROM Movie AS T1 JOIN Rating AS T2 ON T1.mID  =  T2.mID GROUP BY T1.director
SELECT count(*) ,  T1.dormid FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid WHERE T1.student_capacity  >  100 GROUP BY T1.dormid
SELECT count(*) ,  T1.fault_log_entry_id FROM Fault_Log AS T1 JOIN Engineer_Visits AS T2 ON T1.fault_log_entry_id  =  T2.fault_log_entry_id GROUP BY T1.fault_log_entry_id ORDER BY count(*) DESC LIMIT 1
SELECT count(*) ,  T1.maintenance_contract_id FROM Maintenance_Contracts AS T1 JOIN Assets AS T2 ON T1.maintenance_contract_id  =  T2.maintenance_contract_id GROUP BY T1.maintenance_contract_id
SELECT count(*) ,  T1.name FROM accounts AS T1 JOIN checking AS T2 ON T1.custid  =  T2.custid GROUP BY T1.name
SELECT count(*) ,  T1.name FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.src_apid GROUP BY T1.name
SELECT count(*) ,  T1.name FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.src_apid GROUP BY T1.name ORDER BY count(*) DESC
SELECT count(*) ,  T1.name FROM medication AS T1 JOIN prescribes AS T2 ON T1.code = T2.medication GROUP BY T1.brand
SELECT count(*) ,  T1.name FROM projects AS T1 JOIN assignedto AS T2 ON T1.code  =  T2.project GROUP BY T1.name
SELECT count(*) ,  T1.name FROM projects AS T1 JOIN assignedto AS T2 ON T1.code  =  T2.project WHERE T1.hours  >  300 GROUP BY T1.name
SELECT count(*) ,  T1.name FROM scientists AS T1 JOIN assignedto AS T2 ON T1.ssn  =  T2.scientist GROUP BY T1.name
SELECT count(*) ,  T1.product_name FROM product AS T1 JOIN problems AS T2 ON T1.product_id = T2.product_id GROUP BY T1.product_name ORDER BY count(*) DESC LIMIT 1
SELECT count(*) ,  T1.project_details FROM Projects AS T1 JOIN Tasks AS T2 ON T1.project_id  =  T2.project_id GROUP BY T1.project_id
SELECT count(*) ,  T1.school_code FROM department AS T1 JOIN professor AS T2 ON T1.dept_code  =  T2.dept_code GROUP BY T1.school_code
SELECT count(*) ,  T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid GROUP BY T1.stuid
SELECT count(*) ,  T1.year FROM postseason AS T1 JOIN team AS T2 ON T1.team_id_winner  =  T2.team_id_br WHERE T2.name  =  'Boston Red Stockings' GROUP BY T1.year
SELECT count(*) ,  T2.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code GROUP BY T2.name
SELECT count(*) ,  T2.product_id FROM problems AS T1 JOIN product AS T2 ON T1.product_id = T2.product_id GROUP BY T2.product_id
SELECT count(*) ,  T2.product_id FROM problems AS T1 JOIN product AS T2 ON T1.product_id = T2.product_id WHERE T1.date_problem_reported > \"1986-11-13\" GROUP BY T2.product_id
SELECT count(*) ,  T3.dorm_name FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid WHERE T1.age  >  20 GROUP BY T3.dorm_name
SELECT count(*) ,  T3.school_code FROM CLASS AS T1 JOIN course AS T2 ON T1.crs_code  =  T2.crs_code JOIN department AS T3 ON T2.dept_code  =  T3.dept_code GROUP BY T3.school_code
SELECT count(*) ,  YEAR FROM matches GROUP BY YEAR
SELECT count(*) ,  account_id FROM Financial_transactions
SELECT count(*) ,  address FROM member GROUP BY address
SELECT count(*) ,  affiliation FROM university WHERE enrollment  >  20000 GROUP BY affiliation
SELECT count(*) ,  avg(age) ,  city_code FROM student GROUP BY city_code
SELECT count(*) ,  avg(age) ,  city_code FROM student WHERE sex  =  'M' GROUP BY city_code
SELECT count(*) ,  building FROM classroom WHERE capacity  >  50 GROUP BY building
SELECT count(*) ,  cName FROM tryout GROUP BY cName ORDER BY count(*) DESC
SELECT count(*) ,  city FROM airports WHERE country  =  'United States' GROUP BY city ORDER BY count(*) DESC
SELECT count(*) ,  city FROM employee GROUP BY city
SELECT count(*) ,  city FROM employees WHERE title  =  'IT Staff' GROUP BY city
SELECT count(*) ,  city_code FROM student GROUP BY city_code HAVING count(*)  >  1
SELECT count(*) ,  class_room FROM CLASS GROUP BY class_room HAVING count(*)  >=  2
SELECT count(*) ,  classroom FROM list GROUP BY classroom
SELECT count(*) ,  constructorid FROM constructorStandings GROUP BY constructorid
SELECT count(*) ,  country FROM airport GROUP BY country
SELECT count(*) ,  country FROM airports GROUP BY country ORDER BY count(*) DESC
SELECT count(*) ,  country_code FROM players GROUP BY country_code
SELECT count(*) ,  crs_code FROM CLASS GROUP BY crs_code
SELECT count(*) ,  customer_id FROM Accounts GROUP BY customer_id
SELECT count(*) ,  dept_code FROM CLASS AS T1 JOIN course AS T2 ON T1.crs_code  =  T2.crs_code GROUP BY dept_code
SELECT count(*) ,  dept_code FROM professor WHERE prof_high_degree  =  'Ph.D.' GROUP BY dept_code
SELECT count(*) ,  dept_code FROM student GROUP BY dept_code
SELECT count(*) ,  dept_name FROM student GROUP BY dept_name
SELECT count(*) ,  flag FROM ship GROUP BY flag
SELECT count(*) ,  formats FROM files GROUP BY formats
SELECT count(*) ,  gender FROM Person WHERE age  <  40 GROUP BY gender
SELECT count(*) ,  gender FROM artist GROUP BY gender
SELECT count(*) ,  hand FROM players GROUP BY hand
SELECT count(*) ,  major FROM student GROUP BY major
SELECT count(*) ,  nationality FROM constructors GROUP BY nationality
SELECT count(*) ,  project_id FROM Documents WHERE document_type_code  =  \"BK\" GROUP BY project_id
SELECT count(*) ,  rank FROM captain GROUP BY rank
SELECT count(*) ,  rank FROM captain WHERE age  <  50 GROUP BY rank
SELECT count(*) ,  role_code FROM users GROUP BY role_code
SELECT count(*) ,  semester ,  YEAR FROM SECTION GROUP BY semester ,  YEAR
SELECT count(*) ,  sex FROM employee WHERE salary  <  50000 GROUP BY sex
SELECT count(*) ,  sex FROM people WHERE weight  >  85 GROUP BY sex
SELECT count(*) ,  sex FROM student WHERE age  >  (SELECT avg(age) FROM student) GROUP BY sex
SELECT count(*) ,  student_course_id FROM Transcript_Contents GROUP BY student_course_id ORDER BY count(*) DESC LIMIT 1
SELECT count(*) ,  sum(student_capacity) ,  gender FROM dorm GROUP BY gender
SELECT count(*) ,  t1.name FROM manufacturer AS t1 JOIN furniture_manufacte AS t2 ON t1.manufacturer_id  =  t2.manufacturer_id GROUP BY t1.manufacturer_id
SELECT count(*) ,  t1.network_name ,  t1.services FROM station AS t1 JOIN route AS t2 ON t1.id  =  t2.station_id GROUP BY t2.station_id
SELECT count(*) ,  t2.name FROM hiring AS t1 JOIN shop AS t2 ON t1.shop_id  =  t2.shop_id GROUP BY t2.name
SELECT count(*) ,  time_of_day FROM broadcast GROUP BY time_of_day
SELECT count(*) FROM (SELECT * FROM endowment WHERE amount  >  8.5 GROUP BY school_id HAVING count(*)  >  1)
SELECT count(*) FROM (SELECT city FROM airports GROUP BY city HAVING count(*)  >  3)
SELECT count(*) FROM ADDRESSES WHERE zip_postcode  =  \"197\"
SELECT count(*) FROM AIRLINES
SELECT count(*) FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T2.Airline  =  T1.uid WHERE T1.Airline  =  \"United Airlines\" AND T2.DestAirport  =  \"ASY\"
SELECT count(*) FROM AIRLINES AS T1 JOIN FLIGHTS AS T2 ON T2.Airline  =  T1.uid WHERE T1.Airline  =  \"United Airlines\" AND T2.SourceAirport  =  \"AHD\"
SELECT count(*) FROM AIRLINES WHERE Country  =  \"USA\"
SELECT count(*) FROM AIRPORTS
SELECT count(*) FROM ALBUM
SELECT count(*) FROM APPELLATIONS WHERE County  =  \"Napa\"
SELECT count(*) FROM ARTIST WHERE artistid NOT IN(SELECT artistid FROM ALBUM)
SELECT count(*) FROM ASSESSMENT_NOTES
SELECT count(*) FROM Accounts
SELECT count(*) FROM Accounts AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.customer_first_name  =  \"Art\" AND T2.customer_last_name  =  \"Turcotte\"
SELECT count(*) FROM Activity
SELECT count(*) FROM Aircraft
SELECT count(*) FROM Aircraft WHERE distance BETWEEN 1000 AND 5000
SELECT count(*) FROM All_documents WHERE document_type_code  =  \"CV\" OR document_type_code  =  \"BK\"
SELECT count(*) FROM Allergy_type WHERE allergytype  =  \"animal\"
SELECT count(*) FROM Apartment_Bookings
SELECT count(*) FROM Apartments WHERE apt_id NOT IN (SELECT apt_id FROM Apartment_Facilities)
SELECT count(*) FROM BOOKINGS
SELECT count(*) FROM Band
SELECT count(*) FROM CHARACTERISTICS
SELECT count(*) FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code JOIN course AS T3 ON T1.crs_code  =  T3.crs_code JOIN department AS T4 ON T3.dept_code  =  T4.dept_code WHERE T4.dept_name  =  'Accounting'
SELECT count(*) FROM CLASS AS T1 JOIN enroll AS T2 ON T1.class_code  =  T2.class_code WHERE T1.crs_code  =  'ACCT-211'
SELECT count(*) FROM COURSE
SELECT count(*) FROM COURSE WHERE Credits  >  2
SELECT count(*) FROM COURSES
SELECT count(*) FROM CUSTOMERS
SELECT count(*) FROM Church WHERE Open_Date  <  1850
SELECT count(*) FROM College
SELECT count(*) FROM College WHERE enr  >  15000
SELECT count(*) FROM Companies
SELECT count(*) FROM Companies WHERE Industry  =  \"Banking\" OR Industry  =  \"Conglomerate\"
SELECT count(*) FROM Country AS T1 JOIN League AS T2 ON T1.id  =  T2.country_id WHERE T1.name  =  \"England\"
SELECT count(*) FROM Courses
SELECT count(*) FROM Customers
SELECT count(*) FROM Customers WHERE customer_id NOT IN (SELECT customer_id FROM Accounts)
SELECT count(*) FROM Customers_cards
SELECT count(*) FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.customer_first_name  =  \"Art\" AND T2.customer_last_name  =  \"Turcotte\"
SELECT count(*) FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE T2.customer_first_name  =  \"Blanche\" AND T2.customer_last_name  =  \"Huels\" AND T1.card_type_code  =  \"Credit\"
SELECT count(*) FROM Customers_cards WHERE card_type_code  =  \"Debit\"
SELECT count(*) FROM DEPARTMENT WHERE Division  =  \"AS\"
SELECT count(*) FROM Departments AS T1 JOIN Degree_Programs AS T2 ON T1.department_id  =  T2.department_id WHERE T1.department_name  =  'engineer'
SELECT count(*) FROM Documents
SELECT count(*) FROM Documents AS T1 JOIN Templates AS T2 ON T1.Template_ID  =  T2.Template_ID WHERE T2.Template_Type_Code  =  'PPT'
SELECT count(*) FROM Documents WHERE document_id NOT IN ( SELECT document_id FROM Documents_with_expenses )
SELECT count(*) FROM Documents_with_expenses
SELECT count(*) FROM Dogs WHERE age  <  ( SELECT avg(age) FROM Dogs )
SELECT count(*) FROM Dogs WHERE dog_id NOT IN ( SELECT dog_id FROM Treatments )
SELECT count(*) FROM EVENTS WHERE event_id NOT IN (SELECT event_id FROM Participants_in_Events)
SELECT count(*) FROM Employee
SELECT count(*) FROM Employee WHERE salary BETWEEN 100000 AND 200000
SELECT count(*) FROM Employees
SELECT count(*) FROM FLIGHTS
SELECT count(*) FROM FLIGHTS AS T1 JOIN AIRLINES AS T2 ON T1.Airline  =  T2.uid WHERE T2.Airline = \"JetBlue Airways\"
SELECT count(*) FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.DestAirport  =  T2.AirportCode JOIN AIRLINES AS T3 ON T3.uid  =  T1.Airline WHERE T2.City  =  \"Aberdeen\" AND T3.Airline  =  \"United Airlines\"
SELECT count(*) FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.DestAirport  =  T2.AirportCode JOIN AIRPORTS AS T3 ON T1.SourceAirport  =  T3.AirportCode WHERE T2.City  =  \"Ashley\" AND T3.City  =  \"Aberdeen\"
SELECT count(*) FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.DestAirport  =  T2.AirportCode WHERE T2.City  =  \"Aberdeen\"
SELECT count(*) FROM FLIGHTS AS T1 JOIN AIRPORTS AS T2 ON T1.SourceAirport  =  T2.AirportCode WHERE T2.City  =  \"Aberdeen\"
SELECT count(*) FROM FLIGHTS WHERE DestAirport  =  \"ATO\"
SELECT count(*) FROM FLIGHTS WHERE SourceAirport  =  \"APG\"
SELECT count(*) FROM Faculty
SELECT count(*) FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID WHERE T1.fname  =  \"Mark\" AND T1.lname  =  \"Giuliano\"
SELECT count(*) FROM Faculty WHERE Rank  =  \"Professor\" AND building  =  \"NEB\"
SELECT count(*) FROM Faculty WHERE Sex  =  'F' AND Rank  =  \"Professor\"
SELECT count(*) FROM Financial_transactions
SELECT count(*) FROM Financial_transactions AS T1 JOIN Accounts AS T2 ON T1.account_id  =  T2.account_id WHERE T2.account_name  =  \"337\"
SELECT count(*) FROM Flight
SELECT count(*) FROM Flights AS T1 JOIN Airports AS T2 ON T1.DestAirport  =  T2.AirportCode WHERE T2.city  =  \"Aberdeen\" OR T2.city  =  \"Abilene\"
SELECT count(*) FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id WHERE T2.name  =  \"Kyle\"
SELECT count(*) FROM HOTELS
SELECT count(*) FROM Has_allergy AS T1 JOIN Allergy_type AS T2 ON T1.allergy  =  T2.allergy WHERE T2.allergytype  =  \"food\"
SELECT count(*) FROM Has_allergy WHERE Allergy  =  \"Cat\"
SELECT count(*) FROM Highschooler
SELECT count(*) FROM Highschooler WHERE grade  =  9 OR grade  =  10
SELECT count(*) FROM INVESTORS
SELECT count(*) FROM Invoices
SELECT count(*) FROM Lessons AS T1 JOIN Customers AS T2 ON T1.customer_id  = T2.customer_id WHERE T2.first_name = \"Ray\"
SELECT count(*) FROM Lessons AS T1 JOIN Staff AS T2 ON T1.staff_id = T2.staff_id WHERE T2.first_name LIKE \"%a%\"
SELECT count(*) FROM Likes AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id WHERE T2.name  =  \"Kyle\"
SELECT count(*) FROM Movie WHERE YEAR  <  2000
SELECT count(*) FROM Other_Available_Features
SELECT count(*) FROM Owners WHERE owner_id NOT IN ( SELECT owner_id FROM Dogs )
SELECT count(*) FROM Paragraphs
SELECT count(*) FROM Paragraphs AS T1 JOIN Documents AS T2 ON T1.document_ID  =  T2.document_ID WHERE T2.document_name  =  'Summer Show'
SELECT count(*) FROM Person WHERE age  >  (SELECT max(age) FROM person WHERE job  =  'engineer')
SELECT count(*) FROM Person WHERE age  >  30 AND job  =  'engineer'
SELECT count(*) FROM Person WHERE gender  =  'female'
SELECT count(*) FROM Player WHERE HS  >  1000
SELECT count(*) FROM Products
SELECT count(*) FROM Products_for_hire WHERE product_id NOT IN ( SELECT product_id FROM products_booked WHERE booked_amount  >  200 )
SELECT count(*) FROM Professionals WHERE professional_id NOT IN ( SELECT professional_id FROM Treatments )
SELECT count(*) FROM Project_Staff WHERE role_code  =  'leader' OR date_from  <  '1989-04-24 23:51:54'
SELECT count(*) FROM Project_outcomes WHERE outcome_code  =  'Patent'
SELECT count(*) FROM Projects
SELECT count(*) FROM ROLES
SELECT count(*) FROM Rating WHERE stars  >  3
SELECT count(*) FROM Ref_budget_codes
SELECT count(*) FROM Ref_calendar
SELECT count(*) FROM Ref_colors WHERE color_code NOT IN ( SELECT color_code FROM products )
SELECT count(*) FROM Ref_document_types
SELECT count(*) FROM Ref_locations
SELECT count(*) FROM Restaurant JOIN Type_Of_Restaurant ON Restaurant.ResID =  Type_Of_Restaurant.ResID JOIN Restaurant_Type ON Type_Of_Restaurant.ResTypeID = Restaurant_Type.ResTypeID GROUP BY Type_Of_Restaurant.ResTypeID HAVING Restaurant_Type.ResTypeName = 'Sandwich'
SELECT count(*) FROM Reviewer
SELECT count(*) FROM STUDENT
SELECT count(*) FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  Class_Senator_Vote WHERE T1.Sex  =  \"M\" AND T2.Election_Cycle  =  \"Fall\"
SELECT count(*) FROM STUDENT AS T1 JOIN VOTING_RECORD AS T2 ON T1.StuID  =  Class_Senator_Vote WHERE T1.city_code  =  \"NYC\" AND T2.Election_Cycle  =  \"Spring\"
SELECT count(*) FROM STUDENTS WHERE student_id NOT IN ( SELECT student_id FROM Behavior_Incident )
SELECT count(*) FROM Settlements
SELECT count(*) FROM Songs
SELECT count(*) FROM Statements
SELECT count(*) FROM Student
SELECT count(*) FROM Student WHERE age  =  18
SELECT count(*) FROM Student WHERE age  >  18 AND StuID NOT IN ( SELECT StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"food\" OR T2.allergytype  =  \"animal\")
SELECT count(*) FROM Student WHERE city_code  =  \"HKG\" OR city_code  =  \"CHI\"
SELECT count(*) FROM Student WHERE sex  =  \"M\" AND StuID IN (SELECT StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"food\")
SELECT count(*) FROM Student_Tests_Taken WHERE test_result  =  \"Fail\"
SELECT count(*) FROM Students WHERE student_id NOT IN (SELECT student_id FROM Student_Course_Enrolment)
SELECT count(*) FROM Tasks
SELECT count(*) FROM Templates
SELECT count(*) FROM Templates WHERE template_type_code  =  \"CV\"
SELECT count(*) FROM Transcripts
SELECT count(*) FROM VOTING_RECORD
SELECT count(*) FROM Video_games
SELECT count(*) FROM Video_games WHERE gtype  =  \"Massively multiplayer online game\"
SELECT count(*) FROM Visitors WHERE Tourist_ID NOT IN ( SELECT Tourist_ID FROM Visits )
SELECT count(*) FROM WINE WHERE Winery  =  \"Robert Biale\"
SELECT count(*) FROM accounts
SELECT count(*) FROM actor
SELECT count(*) FROM address WHERE district  =  'California'
SELECT count(*) FROM addresses WHERE country  =  'USA'
SELECT count(*) FROM addresses WHERE state_province_county  =  \"Colorado\"
SELECT count(*) FROM aircraft
SELECT count(*) FROM airlines
SELECT count(*) FROM airlines AS T1 JOIN routes AS T2 ON T1.alid  =  T2.alid WHERE T1.name  =  'American Airlines'
SELECT count(*) FROM airlines WHERE country  =  'Russia'
SELECT count(*) FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.dst_apid WHERE T1.name  =  'John F Kennedy International Airport'
SELECT count(*) FROM airports AS T1 JOIN routes AS T2 ON T1.apid  =  T2.dst_apid WHERE country  =  'Canada'
SELECT count(*) FROM airports WHERE name LIKE '%International%'
SELECT count(*) FROM appointment
SELECT count(*) FROM architect WHERE gender  =  'female'
SELECT count(*) FROM area_code_state
SELECT count(*) FROM artist
SELECT count(*) FROM artist WHERE age  >  46 AND year_join  >  1990
SELECT count(*) FROM artist WHERE country  =  \"Bangladesh\"
SELECT count(*) FROM artwork
SELECT count(*) FROM authors
SELECT count(*) FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE  t1.fname  =  \"Atsushi\" AND t1.lname  =  \"Ohori\"
SELECT count(*) FROM bank
SELECT count(*) FROM body_builder
SELECT count(*) FROM book
SELECT count(*) FROM book_club
SELECT count(*) FROM branch WHERE membership_amount  >  (SELECT avg(membership_amount) FROM branch)
SELECT count(*) FROM browser WHERE market_share  >=  5
SELECT count(*) FROM budget WHERE budgeted  <  invested
SELECT count(*) FROM budget WHERE budgeted  >  3000 AND YEAR  <=  2001
SELECT count(*) FROM building
SELECT count(*) FROM building WHERE height_feet  >  (SELECT avg(height_feet) FROM building) OR floors  >  (SELECT avg(floors) FROM building)
SELECT count(*) FROM camera_lens WHERE focal_length_mm  >  15
SELECT count(*) FROM camera_lens WHERE id NOT IN ( SELECT camera_lens_id FROM photos )
SELECT count(*) FROM campuses
SELECT count(*) FROM campuses WHERE county  =  \"Los Angeles\"
SELECT count(*) FROM candidate
SELECT count(*) FROM catalog_contents
SELECT count(*) FROM chip_model WHERE wifi  =  'No'
SELECT count(*) FROM cinema
SELECT count(*) FROM city AS T1 JOIN country AS T2 ON T1.country_id  =  T2.country_id WHERE T2.country  =  'Australia'
SELECT count(*) FROM city WHERE county_ID IN (SELECT county_ID FROM county_public_safety WHERE population  >  20000)
SELECT count(*) FROM claims_processing_stages
SELECT count(*) FROM classroom WHERE building  =  'Lamberton'
SELECT count(*) FROM classroom WHERE building  =  'Lamberton' AND capacity  <  50
SELECT count(*) FROM classroom WHERE building != 'Lamberton'
SELECT count(*) FROM climber
SELECT count(*) FROM club
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\" AND t3.age  <  18
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\" AND t3.age  >  18
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Pen and Paper Gaming\"
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Tennis Club\"
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.fname  =  \"Linda\" AND t3.lname  =  \"Smith\"
SELECT count(*) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.fname  =  \"Tracy\" AND t3.lname  =  \"Kim\"
SELECT count(*) FROM club WHERE clublocation  =  \"HHH\"
SELECT count(*) FROM club_rank WHERE Total  <  10
SELECT count(*) FROM company
SELECT count(*) FROM company WHERE Headquarters  =  'USA'
SELECT count(*) FROM concert WHERE YEAR  =  2014 OR YEAR  =  2015
SELECT count(*) FROM conductor
SELECT count(*) FROM contestants WHERE contestant_number NOT IN ( SELECT contestant_number FROM votes )
SELECT count(*) FROM country
SELECT count(*) FROM country WHERE GovernmentForm  =  \"Republic\"
SELECT count(*) FROM country WHERE continent  =  \"Asia\"
SELECT count(*) FROM country WHERE country_id NOT IN ( SELECT country_id FROM roller_coaster WHERE LENGTH  >  3000 )
SELECT count(*) FROM county
SELECT count(*) FROM county_public_safety
SELECT count(*) FROM course WHERE course_id NOT IN (SELECT course_id FROM prereq)
SELECT count(*) FROM courses AS T1 JOIN student_course_attendance AS T2 ON T1.course_id = T2.course_id WHERE T1.course_name = \"English\"
SELECT count(*) FROM courses AS T1 JOIN student_course_attendance AS T2 ON T1.course_id = T2.course_id WHERE T2.student_id = 171
SELECT count(*) FROM csu_fees WHERE campusfee  >  (SELECT avg(campusfee) FROM csu_fees)
SELECT count(*) FROM customer WHERE active = '1'
SELECT count(*) FROM customer_orders WHERE order_details  =  \"Second time\"
SELECT count(*) FROM customers
SELECT count(*) FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t3.city  =  \"Lake Geovannyton\"
SELECT count(*) FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.customer_name  =  \"Dayana Robel\"
SELECT count(*) FROM customers GROUP BY customer_type_code ORDER BY count(*) DESC LIMIT 1
SELECT count(*) FROM customers WHERE customer_id NOT IN ( SELECT customer_id FROM customer_events )
SELECT count(*) FROM debate
SELECT count(*) FROM department AS T1 JOIN course AS T2 ON T1.dept_code  =  T2.dept_code WHERE dept_name  =  \"Computer Info. Systems\"
SELECT count(*) FROM department_stores AS T1 JOIN department_store_chain AS T2 ON T1.dept_store_chain_id  =  T2.dept_store_chain_id WHERE T2.dept_store_chain_name  =  \"South\"
SELECT count(*) FROM dependent WHERE sex  =  'F'
SELECT count(*) FROM device
SELECT count(*) FROM dorm
SELECT count(*) FROM dorm_amenity
SELECT count(*) FROM driver
SELECT count(*) FROM driver WHERE home_city  =  'Hartford' OR age  <  40
SELECT count(*) FROM editor
SELECT count(*) FROM election
SELECT count(*) FROM employee
SELECT count(*) FROM employee AS T1 JOIN CLASS AS T2 ON T1.EMP_NUM  =  T2.PROF_NUM WHERE T1.EMP_LNAME  =  'Graztevski'
SELECT count(*) FROM entrepreneur
SELECT count(*) FROM enzyme
SELECT count(*) FROM event
SELECT count(*) FROM exhibition WHERE YEAR  >=  2005
SELECT count(*) FROM exhibition_record AS T1 JOIN exhibition AS T2 ON T1.exhibition_id  =  T2.exhibition_id WHERE T1.attendance  >  100 OR T2.ticket_price  <  10
SELECT count(*) FROM farm
SELECT count(*) FROM files WHERE duration LIKE \"4:%\"
SELECT count(*) FROM film
SELECT count(*) FROM film WHERE title LIKE \"%Dummy%\"
SELECT count(*) FROM flight WHERE velocity  >  200
SELECT count(*) FROM follows
SELECT count(*) FROM follows GROUP BY f1
SELECT count(*) FROM game WHERE id NOT IN ( SELECT game_id FROM injury_accident )
SELECT count(*) FROM game WHERE season  >  2007
SELECT count(*) FROM gas_station WHERE open_year BETWEEN 2000 AND 2005
SELECT count(*) FROM gymnast
SELECT count(*) FROM has_allergy AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID WHERE T2.sex  =  \"F\" AND T1.allergy  =  \"Milk\" OR T1.allergy  =  \"Eggs\"
SELECT count(*) FROM head WHERE age  >  56
SELECT count(*) FROM inst
SELECT count(*) FROM institution AS T1 JOIN protein AS T2 ON T1.institution_id  =  T2.institution_id JOIN building AS T3 ON T3.building_id  =  T1.building_id WHERE T3.floors  >=  20
SELECT count(*) FROM institution AS T1 JOIN protein AS T2 ON T1.institution_id  =  T2.institution_id WHERE T1.founded  >  1880 OR T1.type  =  'Private'
SELECT count(*) FROM institution WHERE institution_id NOT IN (SELECT institution_id FROM protein)
SELECT count(*) FROM instruments WHERE instrument  =  \"drums\"
SELECT count(*) FROM inventory WHERE store_id  =  1
SELECT count(*) FROM item WHERE i_id NOT IN (SELECT i_id FROM review)
SELECT count(*) FROM journalist
SELECT count(*) FROM list
SELECT count(*) FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.firstname  =  \"CHRISSY\" AND T1.lastname  =  \"NABOZNY\"
SELECT count(*) FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T1.firstname  =  \"MADLOCK\" AND T1.lastname  =  \"RAY\"
SELECT count(*) FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"KAWA\" AND T2.lastname  =  \"GORDON\"
SELECT count(*) FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"LORIA\" AND T2.lastname  =  \"ONDERSMA\"
SELECT count(*) FROM list AS T1 JOIN teachers AS T2 ON T1.classroom  =  T2.classroom WHERE T2.firstname  =  \"TARRING\" AND T2.lastname  =  \"LEIA\"
SELECT count(*) FROM manufacturers WHERE founder  =  'Andy'
SELECT count(*) FROM manufacturers WHERE headquarter  =  'Tokyo' OR headquarter  =  'Beijing'
SELECT count(*) FROM market WHERE Number_cities  <  300
SELECT count(*) FROM matches
SELECT count(*) FROM matches WHERE YEAR  =  2013 OR YEAR  =  2016
SELECT count(*) FROM medicine WHERE FDA_approved  =  'No'
SELECT count(*) FROM member
SELECT count(*) FROM member WHERE Membership_card  =  'Black'
SELECT count(*) FROM member WHERE address != 'Hartford'
SELECT count(*) FROM museum WHERE open_year  >  2013 OR open_year  <  2008
SELECT count(*) FROM papers
SELECT count(*) FROM participants AS T1 JOIN Participants_in_Events AS T2 ON T1.Participant_ID  =  T2.Participant_ID WHERE T1.participant_details LIKE '%Dr.%'
SELECT count(*) FROM participants WHERE participant_type_code  =  'Organizer'
SELECT count(*) FROM party
SELECT count(*) FROM party_events
SELECT count(*) FROM patient WHERE SSN NOT IN ( SELECT T1.patient FROM Prescribes AS T1 JOIN Medication AS T2 ON T1.Medication  =  T2.Code WHERE T2.name  =  'Procrastin-X' )
SELECT count(*) FROM people WHERE is_male  =  'F' AND age  >  30
SELECT count(*) FROM people WHERE people_id NOT IN (SELECT People_ID FROM body_builder)
SELECT count(*) FROM performance
SELECT count(*) FROM performance AS T1 JOIN band AS T2 ON T1.bandmate  =  T2.id JOIN songs AS T3 ON T3.songid  =  T1.songid WHERE T3.Title  =  \"Flash\"
SELECT count(*) FROM perpetrator
SELECT count(*) FROM pets WHERE weight  >  10
SELECT count(*) FROM phone
SELECT count(*) FROM pilot
SELECT count(*) FROM player
SELECT count(*) FROM players
SELECT count(*) FROM poker_player
SELECT count(*) FROM premises
SELECT count(*) FROM product AS T1 JOIN problems AS T2 ON T1.product_id = T2.product_id WHERE T1.product_name = \"voluptatem\"
SELECT count(*) FROM products
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id JOIN ref_colors AS t4 ON t1.color_code  =  t4.color_code WHERE t4.color_description  =  \"red\" AND t3.characteristic_name  =  \"slow\"
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id JOIN ref_colors AS t4 ON t1.color_code  =  t4.color_code WHERE t4.color_description  =  \"white\" OR t3.characteristic_name  =  \"hot\"
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"flax\"
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"laurel\"
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t3.characteristic_name  =  \"hot\"
SELECT count(*) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id WHERE t1.product_name  =  \"sesame\"
SELECT count(*) FROM products WHERE price >= 180
SELECT count(*) FROM products WHERE product_category_code  =  \"Seeds\"
SELECT count(*) FROM products WHERE product_category_code  =  \"Spices\" AND typical_buying_price  >  1000
SELECT count(*) FROM products WHERE product_id NOT IN ( SELECT product_id FROM Order_items )
SELECT count(*) FROM products WHERE product_price  >  (SELECT avg(product_price) FROM products)
SELECT count(*) FROM professor AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code WHERE DEPT_NAME  =  \"Accounting\"
SELECT count(*) FROM professor AS T1 JOIN department AS T2 ON T1.dept_code  =  T2.dept_code WHERE T2.dept_name  =  'Accounting' OR T2.dept_name  =  'Biology'
SELECT count(*) FROM professor WHERE prof_high_degree  =  'Ph.D.'
SELECT count(*) FROM professor WHERE prof_high_degree  =  'Ph.D.' OR prof_high_degree  =  'MA'
SELECT count(*) FROM program
SELECT count(*) FROM purchase AS T1 JOIN member AS T2 ON T1.member_id  =  T2.member_id WHERE T2.level  =  6
SELECT count(*) FROM race
SELECT count(*) FROM railway
SELECT count(*) FROM ref_colors
SELECT count(*) FROM region
SELECT count(*) FROM region AS t1 JOIN party AS t2 ON t1.region_id  =  t2.region_id JOIN party_events AS t3 ON t2.party_id  =  t3.party_id WHERE t1.region_name  =  \"United Kingdom\" AND t3.Event_Name  =  \"Annaual Meeting\"
SELECT count(*) FROM rent_arrears
SELECT count(*) FROM results AS T1 JOIN races AS T2 ON T1.raceid = T2.raceid WHERE T2.name = \"Australian Grand Prix\" AND YEAR = 2009
SELECT count(*) FROM review
SELECT count(*) FROM roller_coaster
SELECT count(*) FROM rooms WHERE roomid NOT IN (SELECT DISTINCT room FROM reservations)
SELECT count(*) FROM routes AS T1 JOIN airports AS T2 ON T1.dst_apid  =  T2.apid JOIN airlines AS T3 ON T1.alid  =  T3.alid WHERE T2.country  =  'Italy' AND T3.name  =  'American Airlines'
SELECT count(*) FROM routes AS T1 JOIN airports AS T2 ON T1.dst_apid  =  T2.apid WHERE T2.country  =  'Italy'
SELECT count(*) FROM routes WHERE dst_apid IN (SELECT apid FROM airports WHERE country  =  'Canada') AND src_apid IN (SELECT apid FROM airports WHERE country  =  'United States')
SELECT count(*) FROM salary AS T1 JOIN team AS T2 ON T1.team_id  =  T2.team_id_br WHERE T2.name  = 'Boston Red Stockings' AND T1.year  =  2000
SELECT count(*) FROM savings WHERE balance  >  (SELECT avg(balance) FROM savings)
SELECT count(*) FROM school
SELECT count(*) FROM scientists
SELECT count(*) FROM scientists WHERE ssn NOT IN (SELECT scientist FROM AssignedTo)
SELECT count(*) FROM services
SELECT count(*) FROM ship
SELECT count(*) FROM ship WHERE disposition_of_ship  =  'Captured'
SELECT count(*) FROM singer
SELECT count(*) FROM stadium
SELECT count(*) FROM stadium WHERE country != 'Russia'
SELECT count(*) FROM station
SELECT count(*) FROM store
SELECT count(*) FROM storm WHERE Number_Deaths  >  0
SELECT count(*) FROM student
SELECT count(*) FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T2.petid  =  T3.petid WHERE T1.sex  =  'F' AND T3.pettype  =  'dog'
SELECT count(*) FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid WHERE T1.age  >  20
SELECT count(*) FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid WHERE T3.dorm_name  =  'Smith Hall' AND T1.sex  =  'F'
SELECT count(*) FROM student AS T1 JOIN lives_in AS T2 ON T1.stuid  =  T2.stuid JOIN dorm AS T3 ON T3.dormid  =  T2.dormid WHERE T3.gender  =  'M'
SELECT count(*) FROM student WHERE sex  =  'F' AND age  <  25
SELECT count(*) FROM submission
SELECT count(*) FROM teacher
SELECT count(*) FROM technician
SELECT count(*) FROM track
SELECT count(*) FROM train
SELECT count(*) FROM train WHERE name LIKE \"%Express%\"
SELECT count(*) FROM trip AS T1 JOIN station AS T2 ON T1.end_station_id  =  T2.id WHERE T2.city !=  \"San Francisco\"
SELECT count(*) FROM trip AS T1 JOIN station AS T2 ON T1.end_station_id  =  T2.id WHERE T2.city != \"San Francisco\"
SELECT count(*) FROM tryout WHERE decision  =  'yes'
SELECT count(*) FROM tryout WHERE pPos  =  'goalie'
SELECT count(*) FROM tweets
SELECT count(*) FROM university WHERE LOCATION LIKE \"%NY%\"
SELECT count(*) FROM university WHERE school_id NOT IN (SELECT school_id FROM basketball_match)
SELECT count(*) FROM user_profiles
SELECT count(*) FROM useracct
SELECT count(*) FROM useracct WHERE u_id NOT IN (SELECT u_id FROM review)
SELECT count(*) FROM users WHERE user_login  =  1
SELECT count(*) FROM visitor WHERE age  <  30
SELECT count(*) FROM visitor WHERE id NOT IN (SELECT t2.visitor_id FROM museum AS t1 JOIN visit AS t2 ON t1.Museum_ID  =  t2.Museum_ID WHERE t1.open_year  >  2010)
SELECT count(*) FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Demon Kitty Rag\"
SELECT count(*) FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Le Pop\"
SELECT count(*) FROM votes WHERE state  =  'NY' OR state  =  'CA'
SELECT count(*) FROM wedding WHERE YEAR  =  2016
SELECT count(*) FROM wrestler
SELECT count(DISTINCT Birth_Place) FROM people
SELECT count(DISTINCT Company) FROM entrepreneur
SELECT count(DISTINCT Currency_Code) FROM Drama_Workshop_Groups
SELECT count(DISTINCT FDA_approved) FROM medicine
SELECT count(DISTINCT FacID) FROM Faculty_participates_in
SELECT count(DISTINCT GovernmentForm) FROM country WHERE Continent  =  \"Africa\"
SELECT count(DISTINCT Governor) FROM party
SELECT count(DISTINCT Hometown) FROM people
SELECT count(DISTINCT Industry) FROM Companies
SELECT count(DISTINCT LANGUAGE) FROM countrylanguage
SELECT count(DISTINCT LEVEL) FROM member
SELECT count(DISTINCT LOCATION) FROM perpetrator
SELECT count(DISTINCT LOCATION) FROM shop
SELECT count(DISTINCT Name) FROM PHOTOS
SELECT count(DISTINCT Nationality) FROM conductor
SELECT count(DISTINCT Nationality) FROM people
SELECT count(DISTINCT OWNER) FROM channel
SELECT count(DISTINCT POSITION) FROM player
SELECT count(DISTINCT PROF_NUM) FROM CLASS WHERE CRS_CODE  =  \"ACCT-211\"
SELECT count(DISTINCT Party) FROM representative
SELECT count(DISTINCT President_Vote) FROM VOTING_RECORD
SELECT count(DISTINCT RESULT) FROM battle
SELECT count(DISTINCT SOURCE) FROM injury_accident
SELECT count(DISTINCT Software_Platform) FROM device
SELECT count(DISTINCT Status) FROM city
SELECT count(DISTINCT Status_of_Thing_Code) FROM Timed_Status_of_Things
SELECT count(DISTINCT StuID) FROM Plays_games
SELECT count(DISTINCT StuID) FROM Sportsinfo
SELECT count(DISTINCT T1.College) FROM match_season AS T1 JOIN team AS T2 ON T1.Team  =  T2.Team_id WHERE T2.Name  =  \"Columbus Crew\"
SELECT count(DISTINCT T1.Official_native_language) FROM country AS T1 JOIN match_season AS T2 ON T1.Country_id  =  T2.Country WHERE T2.Position  =  \"Defender\"
SELECT count(DISTINCT T1.donator_name) FROM endowment AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T2.school_name  =  \"Glenn\"
SELECT count(DISTINCT T1.injury) FROM injury_accident AS T1 JOIN game AS T2 ON T1.game_id  =  T2.id WHERE T2.season  >  2010
SELECT count(DISTINCT T1.name) ,  T2.Headquarter FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code GROUP BY T2.Headquarter
SELECT count(DISTINCT T2.Delegate) FROM county AS T1 JOIN election AS T2 ON T1.County_id  =  T2.District WHERE T1.Population  >  50000
SELECT count(DISTINCT T2.Language) FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE  IndepYear  <  1930 AND T2.IsOfficial  =  \"T\"
SELECT count(DISTINCT T2.Location_Code) FROM Things AS T1 JOIN Timed_Locations_of_Things AS T2 ON T1.thing_id  =  T2.thing_id WHERE T1.service_details  =  'Unsatisfied'
SELECT count(DISTINCT T2.camera_lens_id) FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id WHERE T1.country  =  'Ethiopia'
SELECT count(DISTINCT T2.id) ,  count(DISTINCT T3.id) ,  T3.dept_name FROM department AS T1 JOIN student AS T2 ON T1.dept_name  =  T2.dept_name JOIN instructor AS T3 ON T1.dept_name  =  T3.dept_name GROUP BY T3.dept_name
SELECT count(DISTINCT T3.title) FROM albums AS T1 JOIN tracklists AS T2 ON T1.aid  =  T2.albumid JOIN songs AS T3 ON T2.songid  =  T3.songid WHERE t1.type  =  \"Studio\"
SELECT count(DISTINCT Team) FROM match_season
SELECT count(DISTINCT UID) FROM tweets
SELECT count(DISTINCT acc_type) FROM customer
SELECT count(DISTINCT advisor) FROM Student
SELECT count(DISTINCT affiliation) FROM university
SELECT count(DISTINCT allergy) FROM Allergy_type
SELECT count(DISTINCT allergytype) FROM Allergy_type
SELECT count(DISTINCT bike_id) FROM trip
SELECT count(DISTINCT brand) FROM camera_lens
SELECT count(DISTINCT cName) FROM tryout
SELECT count(DISTINCT card_type_code) FROM Customers_Cards
SELECT count(DISTINCT city) FROM Person
SELECT count(DISTINCT city) FROM airports WHERE country  =  'Greenland'
SELECT count(DISTINCT city) FROM bank
SELECT count(DISTINCT city) FROM stadium WHERE opening_year  <  2006
SELECT count(DISTINCT city) FROM station
SELECT count(DISTINCT claim_outcome_code) FROM claims_processing
SELECT count(DISTINCT class_code) FROM CLASS
SELECT count(DISTINCT class_section) FROM CLASS WHERE crs_code  =  'ACCT-211'
SELECT count(DISTINCT client) FROM web_client_accelerator
SELECT count(DISTINCT complaint_type_code) FROM complaints
SELECT count(DISTINCT country_code) FROM players
SELECT count(DISTINCT course_id) FROM course WHERE dept_name  =  'Physics'
SELECT count(DISTINCT crs_code) FROM CLASS
SELECT count(DISTINCT current_address_id) FROM Students
SELECT count(DISTINCT customer_id) FROM Accounts
SELECT count(DISTINCT customer_id) FROM customer_orders WHERE order_status  =  \"Cancelled\"
SELECT count(DISTINCT degree_summary_name) FROM Degree_Programs
SELECT count(DISTINCT department_id) FROM Degree_Programs
SELECT count(DISTINCT dept_address) ,  school_code FROM department GROUP BY school_code
SELECT count(DISTINCT dept_address) FROM department WHERE school_code  =  'BUS'
SELECT count(DISTINCT dept_name) ,  school_code FROM department GROUP BY school_code
SELECT count(DISTINCT dept_name) ,  school_code FROM department GROUP BY school_code HAVING count(DISTINCT dept_name)  <  5
SELECT count(DISTINCT dept_name) FROM course
SELECT count(DISTINCT dept_name) FROM department
SELECT count(DISTINCT dog_id) FROM Treatments
SELECT count(DISTINCT dormid) FROM has_amenity
SELECT count(DISTINCT driverId) FROM results WHERE raceId NOT IN( SELECT raceId FROM races WHERE YEAR != 2009 )
SELECT count(DISTINCT dst_apid) FROM airlines AS T1 JOIN routes AS T2 ON T1.alid  =  T2.alid WHERE T1.name  =  'American Airlines'
SELECT count(DISTINCT eid) FROM Certificate
SELECT count(DISTINCT gender) FROM dorm
SELECT count(DISTINCT gtype) FROM Video_games
SELECT count(DISTINCT i_id) FROM review
SELECT count(DISTINCT incident_type_code) FROM Behavior_Incident
SELECT count(DISTINCT instrument) FROM instruments AS T1 JOIN Band AS T2 ON T1.bandmateid  =  T2.id WHERE T2.lastname  =  \"Heilo\"
SELECT count(DISTINCT instrument) FROM instruments AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Badlands\"
SELECT count(DISTINCT instrument) FROM instruments AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Le Pop\"
SELECT count(DISTINCT job) FROM Person
SELECT count(DISTINCT label) FROM albums
SELECT count(DISTINCT language_id) FROM film
SELECT count(DISTINCT last_name) FROM actor
SELECT count(DISTINCT loser_name) FROM matches
SELECT count(DISTINCT major) ,  count(DISTINCT city_code) FROM student
SELECT count(DISTINCT name) FROM PersonFriend WHERE friend NOT IN (SELECT name FROM person WHERE city  =  'Austin')
SELECT count(DISTINCT name) FROM products WHERE name NOT IN (SELECT T1.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code WHERE T2.name  =  'Sony')
SELECT count(DISTINCT name) FROM projects
SELECT count(DISTINCT nationality) FROM swimmer
SELECT count(DISTINCT pPos) FROM tryout
SELECT count(DISTINCT participant_id) FROM participants_in_Events
SELECT count(DISTINCT party_name) FROM party
SELECT count(DISTINCT payment_method) FROM customers
SELECT count(DISTINCT payment_method_code) FROM parties
SELECT count(DISTINCT pettype) FROM pets
SELECT count(DISTINCT product_type_code) FROM products
SELECT count(DISTINCT product_type_code) FROM products_for_hire
SELECT count(DISTINCT professional_id) FROM Treatments
SELECT count(DISTINCT program_id) FROM broadcast WHERE time_of_day  =  'Night'
SELECT count(DISTINCT rank) FROM captain
SELECT count(DISTINCT rating) FROM film
SELECT count(DISTINCT region_id) FROM affected_region
SELECT count(DISTINCT role_code) FROM Project_Staff
SELECT count(DISTINCT roomtype) FROM room
SELECT count(DISTINCT s_id) FROM advisor
SELECT count(DISTINCT school_code) FROM department
SELECT count(DISTINCT school_id) FROM basketball_match
SELECT count(DISTINCT scientist) FROM assignedto
SELECT count(DISTINCT services) FROM station
SELECT count(DISTINCT source_system_code) FROM CMI_cross_references
SELECT count(DISTINCT sportname) FROM Sportsinfo
SELECT count(DISTINCT state) FROM bank
SELECT count(DISTINCT state) FROM college WHERE enr  <  (SELECT avg(enr) FROM college)
SELECT count(DISTINCT state) FROM college WHERE enr  >  (SELECT avg(enr) FROM college)
SELECT count(DISTINCT student_id) FROM Student_Course_Enrolment
SELECT count(DISTINCT student_id) FROM Students_in_Detention
SELECT count(DISTINCT t1.clubname) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.fname  =  \"Eric\" AND t3.lname  =  \"Tai\"
SELECT count(DISTINCT t1.title) FROM papers AS t1 JOIN authorship AS t2 ON t1.paperid  =  t2.paperid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"Tokohu University\"
SELECT count(DISTINCT t1.title) FROM papers AS t1 JOIN authorship AS t2 ON t1.paperid  =  t2.paperid JOIN inst AS t3 ON t2.instid  =  t3.instid WHERE t3.name  =  \"University of Pennsylvania\"
SELECT count(DISTINCT t2.position) FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid WHERE t1.clubname  =  \"Bootup Baltimore\"
SELECT count(DISTINCT t3.characteristic_name) FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"sesame\"
SELECT count(DISTINCT t3.product_id) FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id WHERE t1.customer_name  =  \"Rodrick Heaney\"
SELECT count(DISTINCT template_id) FROM Documents
SELECT count(DISTINCT temporary_acting) FROM management
SELECT count(DISTINCT title) FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE TYPE  =  \"lead\"
SELECT count(DISTINCT title) FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE TYPE  =  \"shared\"
SELECT count(DISTINCT transaction_type) FROM Financial_Transactions
SELECT count(DISTINCT u_id) FROM review
SELECT count(DISTINCT winner_name) FROM matches WHERE tourney_name  =  'WTA Championships' AND winner_hand  =  'L'
SELECT count(T1.SSN) FROM patient AS T1 JOIN prescribes AS T2 ON T1.SSN  =  T2.patient JOIN physician AS T3 ON T2.physician  =  T3.employeeid WHERE T3.name = \"John Dorian\"
SELECT count(T2.friend) FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.name WHERE T1.name  =  'Dan'
SELECT count(course_id) FROM Student_Course_Enrolment
SELECT count(patient) FROM stay WHERE room  =  112
SELECT country ,  count(*) FROM airlines GROUP BY country ORDER BY count(*) DESC
SELECT country ,  count(*) FROM people GROUP BY country
SELECT country ,  count(*) FROM singer GROUP BY country
SELECT country ,  count(*) FROM stadium GROUP BY country
SELECT country FROM TV_Channel EXCEPT SELECT T1.country FROM TV_Channel AS T1 JOIN cartoon AS T2 ON T1.id = T2.Channel WHERE T2.written_by  =  'Todd Casey'
SELECT country FROM addresses GROUP BY country HAVING count(address_id)  >  4
SELECT country FROM airlines GROUP BY country ORDER BY count(*) DESC LIMIT 1
SELECT country FROM airlines WHERE active  =  'Y' GROUP BY country ORDER BY count(*) DESC LIMIT 1
SELECT country FROM airlines WHERE name LIKE 'Orbit%'
SELECT country FROM airport GROUP BY country HAVING count(*)  >  2
SELECT country FROM airports ORDER BY elevation DESC LIMIT 1
SELECT country FROM competition EXCEPT SELECT country FROM competition WHERE competition_type  =  'Friendly'
SELECT country FROM competition WHERE competition_type  =  'Friendly' INTERSECT SELECT country FROM competition WHERE competition_type  =  'Tournament'
SELECT country FROM competition WHERE competition_type  =  'Tournament' GROUP BY country ORDER BY count(*) DESC LIMIT 1
SELECT country FROM people WHERE age  <  25 INTERSECT SELECT country FROM people WHERE age  >  30
SELECT country FROM singer WHERE age  >  40 INTERSECT SELECT country FROM singer WHERE age  <  30
SELECT country FROM stadium EXCEPT SELECT country FROM stadium WHERE opening_year  >  2006
SELECT country FROM stadium GROUP BY country HAVING count(*)  <=  3
SELECT country FROM stadium GROUP BY country ORDER BY count(*) DESC LIMIT 1
SELECT country FROM stadium WHERE capacity  >  60000 INTERSECT SELECT country FROM stadium WHERE capacity  <  50000
SELECT country_code FROM players GROUP BY country_code HAVING count(*)  >  50
SELECT country_code FROM players GROUP BY country_code ORDER BY count(*) DESC LIMIT 1
SELECT country_id ,  COUNT(*) FROM locations GROUP BY country_id
SELECT county ,  count(*) ,  sum(enrollment) FROM school GROUP BY county
SELECT course_description FROM COURSES WHERE course_name  =  \"database\"
SELECT course_description FROM Courses WHERE course_name  =  'math'
SELECT course_id FROM SECTION WHERE semester  =  'Fall' AND YEAR  =  2009 EXCEPT SELECT course_id FROM SECTION WHERE semester  =  'Spring' AND YEAR  =  2010
SELECT course_id FROM SECTION WHERE semester  =  'Fall' AND YEAR  =  2009 INTERSECT SELECT course_id FROM SECTION WHERE semester  =  'Spring' AND YEAR  =  2010
SELECT course_id FROM SECTION WHERE semester  =  'Fall' AND YEAR  =  2009 UNION SELECT course_id FROM SECTION WHERE semester  =  'Spring' AND YEAR  =  2010
SELECT course_id FROM course EXCEPT SELECT course_id FROM prereq
SELECT course_id FROM student_course_registrations WHERE student_id = 121 UNION SELECT course_id FROM student_course_attendance WHERE student_id = 121
SELECT course_name FROM courses ORDER BY course_name
SELECT creation ,  name ,  budget_in_billions FROM department
SELECT creation FROM department GROUP BY creation ORDER BY count(*) DESC LIMIT 1
SELECT crs_credit ,  crs_description FROM course WHERE crs_code  =  'CIS-220'
SELECT crs_credit ,  crs_description FROM course WHERE crs_code  =  'QM-261'
SELECT cust_name ,  acc_bal FROM customer WHERE cust_name LIKE '%a%'
SELECT cust_name ,  acc_type ,  acc_bal FROM customer ORDER BY credit_score DESC LIMIT 1
SELECT cust_name FROM customer EXCEPT SELECT T1.cust_name FROM customer AS T1 JOIN loan AS T2 ON T1.cust_id  =  T2.cust_id WHERE T2.loan_type  =  'Mortgages'
SELECT cust_name FROM customer EXCEPT SELECT cust_name FROM customer WHERE acc_type  =  'saving'
SELECT cust_name FROM customer ORDER BY acc_bal
SELECT cust_name FROM customer ORDER BY credit_score LIMIT 1
SELECT cust_name FROM customer WHERE acc_type  =  'saving' INTERSECT SELECT cust_name FROM customer WHERE acc_type  =  'checking'
SELECT cust_name FROM customer WHERE credit_score  <  (SELECT avg(credit_score) FROM customer)
SELECT customer_details FROM Customers EXCEPT SELECT T1.customer_details FROM Customers AS T1 JOIN Customer_Policies AS T2 ON T1.customer_id  =  T2.customer_id
SELECT customer_details FROM customers EXCEPT SELECT t2.customer_details FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id
SELECT customer_details FROM customers ORDER BY customer_details
SELECT customer_details FROM customers UNION SELECT staff_details FROM staff
SELECT customer_details FROM customers WHERE customer_details LIKE \"%Diana%\"
SELECT customer_email ,  customer_name FROM customers WHERE payment_method  =  'Visa'
SELECT customer_event_id ,  date_moved_in ,  property_id FROM customer_events
SELECT customer_first_name ,  customer_last_name FROM Customers EXCEPT SELECT T1.customer_first_name ,  T1.customer_last_name FROM Customers AS T1 JOIN Accounts AS T2 ON T1.customer_id  =  T2.customer_id
SELECT customer_id ,  count(*) FROM Accounts GROUP BY customer_id
SELECT customer_id ,  count(*) FROM Accounts GROUP BY customer_id ORDER BY count(*) DESC LIMIT 1
SELECT customer_id ,  count(*) FROM Customers_cards GROUP BY customer_id
SELECT customer_id ,  count(*) FROM Customers_cards GROUP BY customer_id ORDER BY count(*) DESC LIMIT 1
SELECT customer_id ,  customer_first_name ,  customer_last_name ,  customer_phone FROM Customers
SELECT customer_id ,  customer_first_name FROM Customers EXCEPT SELECT T1.customer_id ,  T2.customer_first_name FROM Customers_cards AS T1 JOIN Customers AS T2 ON T1.customer_id  =  T2.customer_id WHERE card_type_code  =  \"Credit\"
SELECT customer_id ,  customer_name FROM customers ORDER BY customer_id ASC
SELECT customer_id ,  customer_name FROM customers WHERE customer_address LIKE \"%WY%\" AND payment_method_code != \"Credit Card\"
SELECT customer_id ,  sum(amount_paid) FROM Payments GROUP BY customer_id ORDER BY sum(amount_paid) DESC LIMIT 1
SELECT customer_id FROM Customers EXCEPT SELECT customer_id FROM Accounts
SELECT customer_id FROM customers WHERE customer_name LIKE \"%Diana%\"
SELECT customer_name ,  customer_id FROM customers WHERE customer_address LIKE \"%TN%\"
SELECT customer_name ,  customer_phone ,  customer_email FROM Customers ORDER BY date_became_customer
SELECT customer_name ,  customer_phone ,  payment_method_code FROM customers ORDER BY customer_number DESC
SELECT customer_name ,  customer_phone FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM customer_address_history)
SELECT customer_name ,  customer_phone FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM mailshot_customers)
SELECT customer_name FROM Customers ORDER BY date_became_customer LIMIT 5
SELECT customer_name FROM customers
SELECT customer_name FROM customers EXCEPT SELECT t1.customer_name FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t3.state_province_county  =  'California'
SELECT customer_name FROM customers EXCEPT SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id
SELECT customer_name FROM customers EXCEPT SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id JOIN products AS t4 ON t3.product_id  =  t4.product_id WHERE t4.product_details  =  'Latte'
SELECT customer_name FROM customers EXCEPT SELECT t1.customer_name FROM customers AS t1 JOIN first_notification_of_loss AS t2 ON t1.customer_id  =  t2.customer_id
SELECT customer_name FROM customers WHERE customer_name LIKE \"%Alex%\"
SELECT customer_name FROM customers WHERE payment_method  =  \"Cash\"
SELECT customer_name FROM customers WHERE payment_method  =  (SELECT payment_method FROM customers GROUP BY payment_method ORDER BY count(*) DESC LIMIT 1)
SELECT customer_name FROM customers WHERE payment_method != 'Cash'
SELECT customer_phone ,  customer_email FROM Customers WHERE customer_first_name  =  \"Aniyah\" AND customer_last_name  =  \"Feest\"
SELECT customer_phone FROM available_policies
SELECT customer_phone FROM available_policies WHERE policy_type_code  =  \"Life Insurance\"
SELECT customer_phone FROM available_policies WHERE policy_type_code  =  (SELECT policy_type_code FROM available_policies GROUP BY policy_type_code ORDER BY count(*) DESC LIMIT 1)
SELECT customer_phone FROM customers UNION SELECT supplier_phone FROM suppliers
SELECT customer_status_code ,  cell_mobile_phone_number ,  email_address FROM Customers WHERE first_name = \"Marina\" OR last_name  =  \"Kohler\"
SELECT daily_hire_cost FROM Products_for_hire WHERE product_name LIKE '%Book%'
SELECT date ,  cloud_cover FROM weather ORDER BY cloud_cover DESC LIMIT 5
SELECT date ,  max_temperature_f - min_temperature_f FROM weather ORDER BY max_temperature_f - min_temperature_f LIMIT 1
SELECT date ,  mean_temperature_f ,  mean_humidity FROM weather ORDER BY max_gust_speed_mph DESC LIMIT 3
SELECT date ,  theme ,  sales FROM journal EXCEPT SELECT T1.date ,  T1.theme ,  T1.sales FROM journal AS T1 JOIN journal_committee AS T2 ON T1.journal_ID  =  T2.journal_ID
SELECT date ,  zip_code FROM weather WHERE max_temperature_f  >=  80
SELECT date ,  zip_code FROM weather WHERE min_dew_point_f  <  (SELECT min(min_dew_point_f) FROM weather WHERE zip_code  =  94107)
SELECT date FROM weather WHERE max_temperature_f  >  85
SELECT date FROM weather WHERE mean_sea_level_pressure_inches BETWEEN 30.3 AND 31
SELECT date FROM weather WHERE zip_code  =  94107 AND EVENTS != \"Fog\" AND EVENTS != \"Rain\"
SELECT date_arrived ,  date_departed FROM Dogs
SELECT date_became_customer FROM customers WHERE customer_id BETWEEN 10 AND 20
SELECT date_from ,  date_to FROM Project_Staff WHERE project_id IN( SELECT project_id FROM Project_Staff GROUP BY project_id ORDER BY count(*) DESC LIMIT 1 ) UNION SELECT date_from ,  date_to FROM Project_Staff WHERE role_code  =  'leader'
SELECT date_from ,  date_to FROM Project_Staff WHERE role_code  =  'researcher'
SELECT date_from FROM Project_Staff ORDER BY date_from ASC LIMIT 1
SELECT date_incident_start ,  date_incident_end FROM Behavior_Incident WHERE incident_type_code  =  \"NOISE\"
SELECT date_of_birth FROM Customers WHERE customer_status_code  =  'Good Customer'
SELECT date_of_birth FROM Guests WHERE gender_code  =  \"Male\"
SELECT date_of_enrolment ,  date_of_completion FROM Student_Course_Enrolment
SELECT date_of_latest_logon FROM Students WHERE family_name  =  \"Jaskolski\" OR family_name  =  \"Langosh\"
SELECT date_of_latest_revision FROM Catalogs GROUP BY date_of_latest_revision HAVING count(*)  >  1
SELECT date_of_notes FROM Assessment_Notes
SELECT date_of_transaction ,  share_count FROM TRANSACTIONS
SELECT date_of_transaction FROM TRANSACTIONS WHERE share_count  >  100 OR amount_of_transaction  >  1000
SELECT date_of_transaction FROM TRANSACTIONS WHERE share_count  >=  100 OR amount_of_transaction  >=  100
SELECT date_of_transaction FROM TRANSACTIONS WHERE transaction_type_code  =  \"SALE\"
SELECT date_stored FROM All_documents WHERE Document_name  =  \"Marry CV\"
SELECT date_test_taken FROM Student_Tests_Taken WHERE test_result  =  \"Pass\"
SELECT date_to FROM Project_Staff ORDER BY date_to DESC LIMIT 1
SELECT date_valid_from ,  date_valid_to FROM Customers_cards WHERE card_number  =  \"4560596484842\"
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  \"San Francisco State University\" AND t2.year  =  2001
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  \"San Jose State University\" AND t2.year  =  2000
SELECT department_description FROM Departments WHERE department_name LIKE '%computer%'
SELECT department_id ,  AVG(salary) FROM employees WHERE commission_pct != \"null\" GROUP BY department_id
SELECT department_id ,  SUM(salary) FROM employees GROUP BY department_id HAVING count(*)  >=  2
SELECT department_id FROM employees GROUP BY department_id HAVING COUNT(commission_pct) > 10
SELECT department_id FROM staff_department_assignments GROUP BY department_id ORDER BY count(*) LIMIT 1
SELECT department_name ,  COUNT(*) FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id GROUP BY department_name
SELECT departure_date ,  arrival_date FROM Flight WHERE origin  =  \"Los Angeles\" AND destination  =  \"Honolulu\"
SELECT dept_address FROM department WHERE dept_name  =  'History'
SELECT dept_name ,  AVG (salary) FROM instructor GROUP BY dept_name HAVING AVG (salary)  >  42000
SELECT dept_name ,  budget FROM department WHERE budget  >  (SELECT avg(budget) FROM department)
SELECT dept_name ,  building FROM department ORDER BY budget DESC
SELECT dept_name ,  building FROM department ORDER BY budget DESC LIMIT 1
SELECT dept_name ,  building FROM department WHERE budget  >  (SELECT avg(budget) FROM department)
SELECT dept_name FROM course GROUP BY dept_name ORDER BY count(*) DESC LIMIT 3
SELECT dept_name FROM course GROUP BY dept_name ORDER BY sum(credits) DESC LIMIT 1
SELECT dept_name FROM department ORDER BY budget LIMIT 1
SELECT dept_name FROM department ORDER BY dept_name
SELECT dept_name FROM instructor GROUP BY dept_name ORDER BY avg(salary) DESC LIMIT 1
SELECT dept_name FROM instructor WHERE name LIKE '%Soisalon%'
SELECT dept_name FROM student GROUP BY dept_name ORDER BY count(*) DESC LIMIT 1
SELECT dept_store_chain_id FROM department_stores GROUP BY dept_store_chain_id ORDER BY count(*) DESC LIMIT 2
SELECT destination ,  count(*) FROM Flight GROUP BY destination
SELECT destination FROM Flight GROUP BY destination ORDER BY count(*) LIMIT 1
SELECT detention_summary FROM Detention
SELECT directed_by ,  count(*) FROM film GROUP BY directed_by
SELECT director FROM Movie GROUP BY director HAVING count(*)  =  1
SELECT director FROM Movie WHERE director != \"null\" GROUP BY director HAVING count(*)  =  1
SELECT director FROM Movie WHERE title  = 'Avatar'
SELECT director FROM movie WHERE YEAR  =  1999 OR YEAR  =  2000
SELECT director FROM movie WHERE YEAR  =  2000 INTERSECT SELECT director FROM movie WHERE YEAR  =  1999
SELECT distinct(BillingCountry) FROM INVOICE
SELECT distinct(Famous_Release_date) FROM artist
SELECT distinct(UnitPrice) FROM TRACK
SELECT distinct(catalog_entry_name) FROM catalog_contents
SELECT distinct(catalog_publisher) FROM catalogs WHERE catalog_publisher LIKE \"%Murray%\"
SELECT district FROM address GROUP BY district HAVING count(*)  >=  2
SELECT district FROM shop WHERE Number_products  <  3000 INTERSECT SELECT district FROM shop WHERE Number_products  >  10000
SELECT district_name FROM district ORDER BY city_area ASC LIMIT 1
SELECT district_name FROM district ORDER BY city_population DESC LIMIT 1
SELECT district_name FROM district WHERE city_area  >  10 OR City_Population  >  100000
SELECT dname FROM department ORDER BY mgr_start_date
SELECT document_id ,  count(*) FROM Paragraphs GROUP BY document_id ORDER BY document_id
SELECT document_id ,  document_name ,  document_description FROM Documents
SELECT document_id ,  document_name FROM Documents
SELECT document_id ,  template_id ,  Document_Description FROM Documents WHERE document_name  =  \"Robbin CV\"
SELECT document_id FROM Documents EXCEPT SELECT document_id FROM Documents_with_expenses
SELECT document_id FROM Documents WHERE document_type_code  =  \"CV\" EXCEPT SELECT document_id FROM Documents_with_expenses
SELECT document_id FROM Documents_with_expenses WHERE budget_type_code  =  'SF'
SELECT document_id FROM Paragraphs GROUP BY document_id HAVING count(*)  >=  2
SELECT document_id FROM Paragraphs GROUP BY document_id HAVING count(*) BETWEEN 1 AND 2
SELECT document_id FROM Paragraphs GROUP BY document_id ORDER BY count(*) ASC LIMIT 1
SELECT document_id FROM Paragraphs WHERE paragraph_text  =  'Brazil' INTERSECT SELECT document_id FROM Paragraphs WHERE paragraph_text  =  'Ireland'
SELECT document_name ,  access_count FROM documents ORDER BY access_count DESC LIMIT 1
SELECT document_name ,  access_count FROM documents ORDER BY document_name
SELECT document_name ,  document_date FROM Documents AS T1 JOIN projects AS T2 ON T1.project_id  =  T2.project_id WHERE T2.project_details  =  'Graph Database project'
SELECT document_name ,  document_id FROM Documents WHERE document_type_code  =  \"BK\"
SELECT document_name ,  template_id FROM Documents WHERE Document_Description LIKE \"%w%\"
SELECT document_name FROM documents EXCEPT SELECT t1.document_name FROM documents AS t1 JOIN document_sections AS t2 ON t1.document_code  =  t2.document_code JOIN document_sections_images AS t3 ON t2.section_id  =  t3.section_id
SELECT document_name FROM documents WHERE document_code NOT IN (SELECT document_code FROM document_sections)
SELECT document_name FROM documents WHERE document_name LIKE \"%CV%\"
SELECT document_type_code ,  count(*) FROM Documents GROUP BY document_type_code
SELECT document_type_code ,  document_name ,  document_description FROM Documents WHERE document_name  =  'Noel CV' OR document_name  =  'King Book'
SELECT document_type_code ,  document_type_name ,  document_type_description FROM Ref_document_types
SELECT document_type_code ,  document_type_name FROM Ref_document_types
SELECT document_type_code FROM Document_Types WHERE document_description LIKE 'Initial%'
SELECT document_type_code FROM Documents GROUP BY document_type_code HAVING count(*)  <  3
SELECT document_type_code FROM Documents GROUP BY document_type_code ORDER BY count(*) DESC LIMIT 1
SELECT document_type_code FROM Ref_document_types WHERE document_type_name  =  \"Paper\"
SELECT document_type_code FROM documents GROUP BY document_type_code HAVING count(*)  >  2
SELECT document_type_code FROM documents GROUP BY document_type_code HAVING count(*)  >  4
SELECT document_type_code FROM documents GROUP BY document_type_code HAVING sum(access_count)  >  10000
SELECT document_type_code FROM documents WHERE document_name  =  \"David CV\"
SELECT document_type_description FROM Ref_document_types WHERE document_type_name  =  \"Film\"
SELECT document_type_name ,  document_type_description FROM Ref_document_types WHERE document_type_code  =  \"RV\"
SELECT donator_name ,  sum(amount) FROM endowment GROUP BY donator_name ORDER BY sum(amount) DESC
SELECT donator_name FROM endowment EXCEPT SELECT donator_name FROM endowment WHERE amount  <  9
SELECT dorm_name ,  gender FROM dorm WHERE student_capacity  >  300 OR student_capacity  <  100
SELECT dorm_name FROM dorm EXCEPT SELECT T1.dorm_name FROM dorm AS T1 JOIN has_amenity AS T2 ON T1.dormid  =  T2.dormid JOIN dorm_amenity AS T3 ON T2.amenid  =  T3.amenid WHERE T3.amenity_name  =  'TV Lounge'
SELECT dorm_name FROM dorm ORDER BY student_capacity DESC LIMIT 1
SELECT dorm_name FROM dorm WHERE dormid NOT IN (SELECT dormid FROM has_amenity)
SELECT dorm_name FROM dorm WHERE gender  =  'F'
SELECT dorm_name FROM dorm WHERE student_capacity  >  300
SELECT eid ,  name FROM Employee ORDER BY salary DESC LIMIT 1
SELECT eid ,  salary FROM Employee WHERE name  =  'Mark Young'
SELECT eid FROM Employee EXCEPT SELECT eid FROM Certificate
SELECT eid FROM Employee WHERE salary  >  100000
SELECT email FROM employees WHERE commission_pct  =  \"null\" AND salary BETWEEN 7000 AND 12000 AND department_id = 50
SELECT email FROM user_profiles WHERE name  =  'Mary'
SELECT email_address ,  cell_number ,  home_phone FROM professionals
SELECT email_address ,  date_of_birth FROM Customers WHERE first_name = \"Carole\"
SELECT email_address ,  phone_number FROM customers ORDER BY email_address ,  phone_number
SELECT email_address ,  phone_number FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM complaints)
SELECT email_address FROM Professionals WHERE state  =  'Hawaii' OR state  =  'Wisconsin'
SELECT email_address FROM Students WHERE first_name  =  \"Emma\" AND last_name  =  \"Rohan\"
SELECT emp_fname ,  emp_lname FROM employee ORDER BY emp_dob LIMIT 1
SELECT emp_fname FROM employee WHERE emp_jobcode  =  'PROF' EXCEPT SELECT T1.emp_fname FROM employee AS T1 JOIN CLASS AS T2 ON T1.emp_num  =  T2.prof_num
SELECT emp_fname FROM employee WHERE emp_jobcode  =  'PROF' ORDER BY emp_dob
SELECT emp_jobcode ,  count(*) FROM employee GROUP BY emp_jobcode ORDER BY count(*) DESC LIMIT 1
SELECT employee_ID FROM Employees WHERE employee_name  =  \"Ebba\"
SELECT employee_id ,  MAX(end_date) FROM job_history GROUP BY employee_id
SELECT employee_id ,  first_name ,  last_name ,  job_id FROM employees WHERE salary  >  ( SELECT max(salary) FROM employees WHERE job_id  =  'PU_MAN' )
SELECT employee_id ,  first_name ,  last_name ,  salary FROM employees WHERE salary  >  ( SELECT AVG (salary) FROM employees ) AND department_id IN ( SELECT department_id FROM employees WHERE first_name LIKE '%J%')
SELECT employee_id ,  first_name ,  last_name FROM employees WHERE department_id IN ( SELECT department_id FROM employees WHERE first_name LIKE '%T%' )
SELECT employee_id ,  job_id FROM employees WHERE salary  <  ( SELECT min(salary) FROM employees WHERE job_id  =  'MK_MAN' )
SELECT employee_id ,  salary FROM employees WHERE manager_id  =  (SELECT employee_id FROM employees WHERE first_name  =  'Payam' )
SELECT employee_id FROM Employees EXCEPT SELECT Destroyed_by_Employee_ID FROM Documents_to_be_destroyed
SELECT employee_id FROM Employees EXCEPT SELECT Destruction_Authorised_by_Employee_ID FROM Documents_to_be_destroyed
SELECT employee_id FROM employees WHERE salary  >  (SELECT AVG(salary) FROM employees)
SELECT employee_id FROM job_history GROUP BY employee_id HAVING COUNT(*)  >= 2
SELECT employee_name ,  role_code ,  date_of_birth FROM Employees WHERE employee_Name  =  'Armani'
SELECT employee_name FROM Employees EXCEPT SELECT Employees.employee_name FROM Employees JOIN Circulation_History ON Circulation_History.employee_id = Employees.employee_id
SELECT employee_name FROM Employees WHERE role_code  =  \"HR\"
SELECT enrollment ,  primary_conference FROM university ORDER BY founded LIMIT 1
SELECT event_id FROM EVENTS EXCEPT SELECT T1.event_id FROM Participants_in_Events AS T1 JOIN Participants AS T2 ON T1.Participant_ID  =  T2.Participant_ID WHERE Participant_Details  =  'Kenyatta Kuhn'
SELECT event_name FROM party_events GROUP BY event_name HAVING count(*)  >  2
SELECT f_id ,  genre_is ,  artist_name FROM song WHERE languages  =   \"english\" ORDER BY rating
SELECT f_id FROM files ORDER BY duration DESC LIMIT 1
SELECT f_id FROM files WHERE formats  =  \"mp3\"
SELECT f_id FROM files WHERE formats  =  \"mp4\" INTERSECT SELECT f_id FROM song WHERE resolution  <  1000
SELECT f_id FROM files WHERE formats  =  \"mp4\" UNION SELECT f_id FROM song WHERE resolution  >  720
SELECT f_id FROM song WHERE resolution  >  (SELECT avg(resolution) FROM song WHERE genre_is  =  \"modern\")
SELECT f_id FROM song WHERE resolution  >  (SELECT max(resolution) FROM song WHERE rating  <  8)
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2002 AND T2.campus  =  \"Long Beach State University\"
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2004 AND T2.campus  =  \"San Francisco State University\"
SELECT first_name ,   last_name ,   salary ,  department_id ,  MAX(salary) FROM employees GROUP BY department_id
SELECT first_name ,  birth_date FROM players WHERE country_code  =  'USA'
SELECT first_name ,  country_code FROM players ORDER BY birth_date LIMIT 1
SELECT first_name ,  department_id FROM employees WHERE last_name  =  'McEwen'
SELECT first_name ,  last_name ,  department_id FROM employees WHERE salary IN (SELECT MIN(salary) FROM employees GROUP BY department_id)
SELECT first_name ,  last_name ,  email_address FROM Owners WHERE state LIKE '%North%'
SELECT first_name ,  last_name ,  gender_mf FROM customers WHERE good_or_bad_customer  =  'good' ORDER BY last_name
SELECT first_name ,  last_name ,  hire_date ,  salary ,  department_id FROM employees WHERE first_name NOT LIKE '%M%'
SELECT first_name ,  last_name ,  hire_date ,  salary ,  department_id FROM employees WHERE first_name NOT LIKE '%M%' ORDER BY department_id
SELECT first_name ,  last_name ,  hire_date FROM employees WHERE department_id  =  ( SELECT department_id FROM employees WHERE first_name  =  \"Clara\") AND first_name != \"Clara\"
SELECT first_name ,  last_name ,  hire_date FROM employees WHERE department_id  =  (SELECT department_id FROM employees WHERE first_name  =  \"Clara\")
SELECT first_name ,  last_name ,  salary FROM  employees WHERE commission_pct  =  \"null\"
SELECT first_name ,  last_name ,  salary FROM employees AS T1 JOIN departments AS T2 ON T1.department_id  =  T2.department_id JOIN  locations AS T3 ON T2.location_id  =  T3.location_id WHERE  T3.city  =  'London'
SELECT first_name ,  last_name ,  salary FROM employees WHERE first_name LIKE '%m'
SELECT first_name ,  last_name ,  salary FROM employees WHERE salary  <  6000
SELECT first_name ,  last_name FROM Maintenance_Engineers WHERE engineer_id NOT IN (SELECT engineer_id FROM Engineer_Visits)
SELECT first_name ,  last_name FROM Teachers ORDER BY last_name
SELECT first_name ,  last_name FROM actor GROUP BY first_name ,  last_name ORDER BY count(*) DESC LIMIT 1
SELECT first_name ,  last_name FROM employees WHERE department_id  =  70 OR department_id  =  90
SELECT first_name ,  last_name FROM employees WHERE salary  >  (SELECT salary FROM employees WHERE employee_id = 163 )
SELECT first_name ,  last_name FROM players ORDER BY birth_date
SELECT first_name ,  last_name FROM players WHERE hand  =  'L' ORDER BY birth_date
SELECT first_name ,  middle_name ,  last_name FROM Students ORDER BY date_first_registered ASC LIMIT 1
SELECT first_name ,  middle_name ,  last_name FROM Students ORDER BY date_left ASC LIMIT 1
SELECT first_name FROM Professionals UNION SELECT first_name FROM Owners EXCEPT SELECT name FROM Dogs
SELECT first_name FROM Staff EXCEPT SELECT T2.first_name FROM Lessons AS T1 JOIN Staff AS T2 ON T1.staff_id  =  T2.staff_id
SELECT first_name FROM Students WHERE current_address_id != permanent_address_id
SELECT first_name FROM Teachers WHERE email_address LIKE '%man%'
SELECT first_name FROM actor GROUP BY first_name ORDER BY count(*) DESC LIMIT 1
SELECT first_name FROM customer WHERE customer_id NOT IN( SELECT customer_id FROM rental WHERE rental_date  >  '2005-08-23 02:06:01' )
SELECT first_name FROM people ORDER BY first_name
SELECT firstname FROM list WHERE classroom  =  108
SELECT firstname FROM teachers WHERE classroom  =  110
SELECT flag FROM ship GROUP BY flag ORDER BY count(*) DESC LIMIT 1
SELECT flno ,  distance FROM Flight ORDER BY price DESC LIMIT 1
SELECT flno ,  origin ,  destination FROM Flight ORDER BY origin
SELECT flno FROM Flight ORDER BY distance ASC LIMIT 3
SELECT flno FROM Flight WHERE distance  >  2000
SELECT flno FROM Flight WHERE origin  =  \"Los Angeles\"
SELECT fname ,  age FROM Student WHERE StuID IN (SELECT StuID FROM Sportsinfo WHERE SportName  =  \"Football\" INTERSECT SELECT StuID FROM Sportsinfo WHERE SportName  =  \"Lacrosse\")
SELECT fname ,  lname ,  address FROM employee ORDER BY Bdate
SELECT fname ,  lname FROM Faculty WHERE Rank  =  \"Instructor\"
SELECT fname ,  lname FROM Student WHERE StuID NOT IN (SELECT T1.StuID FROM Has_allergy AS T1 JOIN Allergy_Type AS T2 ON T1.Allergy  =  T2.Allergy WHERE T2.allergytype  =  \"food\")
SELECT fname ,  lname FROM authors ORDER BY lname
SELECT fname ,  lname FROM employee WHERE salary  >  30000
SELECT fname ,  lname FROM student WHERE age  <  (SELECT avg(age) FROM student)
SELECT fname ,  lname FROM student WHERE city_code != 'HKG' ORDER BY age
SELECT fname ,  lname FROM student WHERE major != (SELECT major FROM student GROUP BY major ORDER BY count(*) DESC LIMIT 1)
SELECT fname ,  major FROM Student WHERE StuID NOT IN (SELECT StuID FROM Has_allergy WHERE Allergy  =  \"Soy\")
SELECT fname ,  sex FROM Student WHERE StuID IN (SELECT StuID FROM Has_allergy WHERE Allergy  =  \"Milk\" EXCEPT SELECT StuID FROM Has_allergy WHERE Allergy  =  \"Cat\")
SELECT fname FROM authors ORDER BY fname
SELECT fname FROM authors WHERE lname  =  \"Ueno\"
SELECT fname FROM student WHERE age  >  20
SELECT fname FROM student WHERE city_code  =  'PHL' AND age BETWEEN 20 AND 25
SELECT forename ,  surname FROM drivers WHERE nationality = \"German\"
SELECT formats FROM files GROUP BY formats ORDER BY COUNT (*) DESC LIMIT 1
SELECT founded FROM university ORDER BY enrollment DESC LIMIT 1
SELECT founded FROM university WHERE affiliation != 'Public' ORDER BY founded DESC LIMIT 1
SELECT founder FROM manufacturers WHERE name  =  'Sony'
SELECT founder FROM manufacturers WHERE name LIKE 'S%'
SELECT friend FROM PersonFriend WHERE name =  'Zach' AND YEAR  =  (SELECT max(YEAR) FROM PersonFriend WHERE name =  'Zach')
SELECT g_name ,  rating FROM genre ORDER BY g_name
SELECT gameid ,  sum(hours_played) FROM Plays_games GROUP BY gameid
SELECT gdp FROM city ORDER BY Regional_Population DESC LIMIT 1
SELECT gender ,  count(*) FROM Customers GROUP BY gender
SELECT gender FROM TEACHERS WHERE last_name  =  \"Medhurst\"
SELECT gender FROM staff GROUP BY gender ORDER BY count(*) DESC LIMIT 1
SELECT gender_code ,  COUNT(*) FROM Guests GROUP BY gender_code ORDER BY COUNT(*) DESC
SELECT gname ,  gtype FROM Video_games ORDER BY gname
SELECT gname FROM Plays_games AS T1 JOIN Video_games AS T2 ON T1.gameid  =  T2.gameid GROUP BY T1.gameid HAVING sum(hours_played)  >=  1000
SELECT gname FROM Plays_games AS T1 JOIN Video_games AS T2 ON T1.gameid  =  T2.gameid GROUP BY T1.gameid ORDER BY sum(hours_played) DESC LIMIT 1
SELECT gname FROM Video_games WHERE gtype  =  \"Collectible card game\"
SELECT grade ,  count(*) FROM Highschooler GROUP BY grade
SELECT grade ,  count(DISTINCT classroom) ,  count(*) FROM list GROUP BY grade
SELECT grade FROM Highschooler
SELECT grade FROM Highschooler GROUP BY grade HAVING count(*)  >=  4
SELECT grade FROM Highschooler GROUP BY grade ORDER BY count(*) DESC LIMIT 1
SELECT grade FROM Highschooler WHERE name  =  \"Kyle\"
SELECT grant_id ,  count(*) FROM Documents GROUP BY grant_id ORDER BY count(*) DESC LIMIT 1
SELECT gtype ,  count(*) FROM Video_games GROUP BY gtype
SELECT gtype FROM Video_games GROUP BY gtype ORDER BY count(*) DESC LIMIT 1
SELECT gtype FROM Video_games GROUP BY gtype ORDER BY count(*) LIMIT 1
SELECT gtype FROM Video_games WHERE gname  =  \"Call of Destiny\"
SELECT guest_first_name ,  guest_last_name FROM Guests
SELECT head_id ,  name FROM head WHERE name LIKE '%Ha%'
SELECT headquarter FROM manufacturers WHERE founder  =  'James'
SELECT headquarters ,  count(*) FROM company GROUP BY headquarters
SELECT headquarters FROM company EXCEPT SELECT headquarters FROM company WHERE main_industry  =  'Banking'
SELECT headquarters FROM company WHERE main_industry  =  'Banking' GROUP BY headquarters HAVING count(*)  >=  2
SELECT headquarters FROM company WHERE main_industry  =  'Banking' INTERSECT SELECT headquarters FROM company WHERE main_industry  =  'Oil and gas'
SELECT hire_date FROM employees WHERE first_name NOT LIKE '%M%'
SELECT home_city FROM driver EXCEPT SELECT home_city FROM driver WHERE age  >  40
SELECT home_city FROM driver GROUP BY home_city ORDER BY count(*) DESC LIMIT 1
SELECT home_city FROM driver WHERE age  >  40 GROUP BY home_city HAVING count(*)  >=  2
SELECT host_city FROM hosting_city ORDER BY YEAR DESC LIMIT 1
SELECT hotel_id ,  star_rating_code FROM HOTELS ORDER BY price_range ASC
SELECT i_id FROM advisor AS T1 JOIN student AS T2 ON T1.s_id  =  T2.id WHERE T2.dept_name  =  'History'
SELECT i_id FROM item WHERE title  =  \"orange\"
SELECT id ,  color ,  name FROM photos
SELECT id ,  country ,  city ,  name FROM airport ORDER BY name
SELECT id ,  duration FROM trip ORDER BY duration DESC LIMIT 3
SELECT id ,  market_share FROM browser WHERE name  =  'Safari'
SELECT id ,  name ,  market_share FROM browser
SELECT id FROM Highschooler EXCEPT SELECT student_id FROM Friend
SELECT id FROM TV_Channel EXCEPT SELECT channel FROM cartoon WHERE directed_by  =  'Ben Jones'
SELECT id FROM instructor EXCEPT SELECT id FROM teaches
SELECT id FROM station WHERE city  =  \"San Francisco\" INTERSECT SELECT station_id FROM status GROUP BY station_id HAVING avg(bikes_available)  >  10
SELECT id FROM station WHERE lat  >  37.4 EXCEPT SELECT station_id FROM status GROUP BY station_id HAVING min(bikes_available)  <  7
SELECT id FROM teaches WHERE semester  =  'Fall' AND YEAR  =  2009 EXCEPT SELECT id FROM teaches WHERE semester  =  'Spring' AND YEAR  =  2010
SELECT id FROM trip ORDER BY duration LIMIT 1
SELECT id FROM trip WHERE duration  >=  (SELECT avg(duration) FROM trip WHERE zip_code  =  94103)
SELECT id FROM tv_channel GROUP BY country HAVING count(*)  >  2
SELECT image_name ,  image_url FROM images ORDER BY image_name
SELECT incident_type_description FROM Ref_Incident_Type WHERE incident_type_code  =  \"VIOLENCE\"
SELECT individual_first_name ,  individual_middle_name ,  individual_last_name FROM individuals ORDER BY individual_last_name
SELECT institution ,  LOCATION FROM institution WHERE founded  >  1990 AND TYPE  =  'Private'
SELECT instrument FROM instruments AS T1 JOIN Band AS T2 ON T1.bandmateid  =  T2.id WHERE T2.lastname  =  \"Heilo\"
SELECT instrument FROM instruments AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid WHERE title  =  \"Le Pop\"
SELECT instrument FROM instruments GROUP BY instrument ORDER BY count(*) DESC LIMIT 1
SELECT interaction_type ,  count(*) FROM medicine_enzyme_interaction GROUP BY interaction_type ORDER BY count(*) DESC LIMIT 1
SELECT investor_id ,  COUNT(*) FROM TRANSACTIONS GROUP BY investor_id
SELECT investor_id ,  COUNT(*) FROM TRANSACTIONS WHERE transaction_type_code  =  \"SALE\" GROUP BY investor_id
SELECT investor_id ,  avg(amount_of_transaction) FROM TRANSACTIONS GROUP BY investor_id
SELECT investor_id ,  avg(share_count) FROM TRANSACTIONS GROUP BY investor_id
SELECT investor_id ,  avg(share_count) FROM TRANSACTIONS GROUP BY investor_id ORDER BY avg(share_count)
SELECT invoice_number ,  count(*) FROM Financial_transactions GROUP BY invoice_number
SELECT invoice_number FROM invoices WHERE invoice_date < \"1989-09-03\" OR invoice_date > \"2007-12-25\"
SELECT job_id ,  hire_date FROM employees WHERE hire_date BETWEEN '2007-11-05' AND '2009-07-05'
SELECT job_id FROM employees GROUP BY job_id HAVING AVG(salary) > 8000
SELECT job_id FROM job_history WHERE end_date - start_date  > 300 GROUP BY job_id HAVING COUNT(*) >= 2
SELECT job_title ,  AVG(salary) FROM employees AS T1 JOIN jobs AS T2 ON T1.job_id  =  T2.job_id GROUP BY T2.job_title
SELECT job_title ,  max_salary - min_salary FROM jobs WHERE max_salary BETWEEN 12000 AND 18000
SELECT job_title FROM jobs WHERE min_salary  >  9000
SELECT label FROM albums GROUP BY label ORDER BY count(*) DESC LIMIT 1
SELECT languages FROM song GROUP BY languages ORDER BY count(*) DESC LIMIT 1
SELECT last_name FROM Customers INTERSECT SELECT last_name FROM Staff
SELECT last_name FROM Teachers EXCEPT SELECT T1.last_name FROM Teachers AS T1 JOIN Detention AS T2 ON T1.teacher_id  =  T2.teacher_id
SELECT last_name FROM staff WHERE email_address LIKE \"%wrau%\"
SELECT lastname FROM list WHERE classroom  =  111
SELECT lastname FROM teachers WHERE classroom  =  109
SELECT line_1 ,  line_2 FROM addresses
SELECT lname ,  age FROM Student WHERE StuID IN (SELECT StuID FROM Has_allergy WHERE Allergy  =  \"Milk\" INTERSECT SELECT StuID FROM Has_allergy WHERE Allergy  =  \"Cat\")
SELECT lname ,  sex FROM Student WHERE StuID IN (SELECT T1.StuID FROM Plays_games AS T1 JOIN Video_games AS T2 ON T1.GameID  =  T2.GameID WHERE T2.Gname  =  \"Call of Destiny\" INTERSECT SELECT T1.StuID FROM Plays_games AS T1 JOIN Video_games AS T2 ON T1.GameID  =  T2.GameID WHERE T2.Gname  =  \"Works of Widenius\")
SELECT lname FROM authors ORDER BY lname
SELECT lname FROM authors WHERE fname  =  \"Amal\"
SELECT lname FROM faculty WHERE rank  =  'Professor' EXCEPT SELECT DISTINCT T1.lname FROM Faculty AS T1 JOIN Faculty_participates_in AS T2 ON T1.facID  =  T2.facID JOIN activity AS T3 ON T2.actid  =  T2.actid WHERE T3.activity_name  =  'Canoeing' OR T3.activity_name  =  'Kayaking'
SELECT lname FROM student WHERE sex  =  'F' AND city_code  =  'BAL' UNION SELECT lname FROM student WHERE sex  =  'M' AND age  <  20
SELECT local_authority ,  services FROM station
SELECT location_code ,  count(*) FROM Document_locations GROUP BY location_code
SELECT location_code ,  date_in_location_from ,  date_in_locaton_to FROM Document_locations
SELECT location_code ,  location_name FROM Ref_locations
SELECT location_code FROM Document_locations GROUP BY location_code HAVING count(*)  >=  3
SELECT location_code FROM Document_locations GROUP BY location_code ORDER BY count(*) DESC LIMIT 1
SELECT location_code FROM Ref_locations WHERE location_name  =  \"Canada\"
SELECT location_name ,  location_description FROM Ref_locations WHERE location_code  =  \"x\"
SELECT login_name ,  family_name FROM Course_Authors_and_Tutors
SELECT login_name FROM Course_Authors_and_Tutors INTERSECT SELECT login_name FROM Students
SELECT login_name FROM Students WHERE family_name  =  \"Ward\"
SELECT lot_details ,  investor_id FROM LOTS
SELECT lot_details FROM Lots EXCEPT SELECT T1.lot_details FROM Lots AS T1 JOIN transactions_lots AS T2 ON T1.lot_id  =  T2.lot_id
SELECT mID ,  avg(stars) FROM Rating GROUP BY mID HAVING count(*)  >=  2
SELECT mID ,  avg(stars) FROM Rating WHERE mID NOT IN (SELECT T1.mID FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID WHERE T2.name  =  \"Brittany Harris\") GROUP BY mID
SELECT mID FROM Rating EXCEPT SELECT T1.mID FROM Rating AS T1 JOIN Reviewer AS T2 ON T1.rID  =  T2.rID WHERE T2.name  =  \"Brittany Harris\"
SELECT main_industry ,  count(*) FROM company GROUP BY main_industry ORDER BY sum(market_value) DESC LIMIT 1
SELECT main_industry ,  sum(market_value) FROM company GROUP BY main_industry
SELECT major ,  age FROM student WHERE stuid NOT IN (SELECT T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat')
SELECT major ,  avg(age) ,  min(age) ,  max(age) FROM Student GROUP BY major
SELECT major ,  count(*) FROM Student GROUP BY major
SELECT major FROM Student GROUP BY major ORDER BY count(*) DESC LIMIT 1
SELECT manager_name ,  district FROM shop ORDER BY number_products DESC LIMIT 1
SELECT manager_name FROM gas_station WHERE open_year  >  2000 GROUP BY manager_name ORDER BY count(*) DESC LIMIT 1
SELECT mascot FROM school WHERE enrollment  >  (SELECT avg(enrollment) FROM school)
SELECT match_id FROM MATCH WHERE competition = \"1994 FIFA World Cup qualification\"
SELECT max(Account_details) FROM Accounts UNION SELECT Account_details FROM Accounts WHERE Account_details LIKE \"%5%\"
SELECT max(Age) ,  min(Age) FROM STUDENT WHERE Major  =  600
SELECT max(Age) FROM STUDENT
SELECT max(Cows) ,  min(Cows) FROM farm
SELECT max(Enrollment) FROM university
SELECT max(Final_Table_Made) FROM poker_player WHERE Earnings  <  200000
SELECT max(LEVEL) FROM manager WHERE Country != \"Australia      \"
SELECT max(Market_Value_in_Billion) ,  min(Market_Value_in_Billion) FROM company
SELECT max(Milliseconds) ,  min(Milliseconds) FROM GENRE AS T1 JOIN TRACK AS T2 ON T1.GenreId  =  T2.GenreId WHERE T1.Name  =  \"Pop\"
SELECT max(Milliseconds) ,  min(Milliseconds) FROM TRACK
SELECT max(Number_cities) ,  min(Number_cities) FROM market
SELECT max(OMIM) FROM enzyme
SELECT max(Points) FROM climber WHERE Country  =  \"United Kingdom\"
SELECT max(Population) ,  min(Population) FROM county
SELECT max(Price) ,  max(Score) ,  YEAR FROM WINE GROUP BY YEAR
SELECT max(Price) ,  max(Score) FROM WINE WHERE Appelation  =  \"St. Helena\"
SELECT max(SHARE) ,  min(SHARE) FROM performance WHERE TYPE != \"Live final\"
SELECT max(Sales_billion) ,  min(Sales_billion) FROM Companies WHERE Industry != \"Banking\"
SELECT max(Silver) ,  min(Silver) FROM club_rank
SELECT max(T1.HS) ,  pPos FROM player AS T1 JOIN tryout AS T2 ON T1.pID  =  T2.pID WHERE T1.HS  >  1000 GROUP BY T2.pPos
SELECT max(T1.duration) ,  max(T2.resolution) ,  T2.languages FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id GROUP BY T2.languages ORDER BY T2.languages
SELECT max(T1.length_meters) ,  T2.name FROM bridge AS T1 JOIN architect AS T2 ON T1.architect_id  =  T2.id
SELECT max(T1.stars) ,  T2.year FROM Rating AS T1 JOIN Movie AS T2 ON T1.mID  =  T2.mID WHERE T2.year  =  (SELECT max(YEAR) FROM Movie)
SELECT max(T2.Price) FROM APPELLATIONS AS T1 JOIN WINE AS T2 ON T1.Appelation  =  T2.Appelation WHERE T1.Area  =  \"Central Coast\" AND T2.year  <  2005
SELECT max(T2.fastestlapspeed) ,  T1.name ,  T1.year FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid WHERE T1.year > 2014 GROUP BY T1.name ORDER BY T1.year
SELECT max(T2.fastestlapspeed) FROM races AS T1 JOIN results AS T2 ON T1.raceid = T2.raceid WHERE T1.year = 2008 AND T1.name = \"Monaco Grand Prix\"
SELECT max(T2.resolution) ,  min(T2.resolution) FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id WHERE T1.duration LIKE \"3:%\"
SELECT max(Transit_Passengers) ,  min(Transit_Passengers) FROM airport
SELECT max(Weeks_on_Top) ,  min(Weeks_on_Top) FROM volume
SELECT max(age) FROM Dogs
SELECT max(amount_of_transaction) FROM TRANSACTIONS
SELECT max(area_code) ,  min(area_code) FROM area_code_state
SELECT max(booked_count) ,  min(booked_count) ,  avg(booked_count) FROM products_booked
SELECT max(budget_in_billions) ,  min(budget_in_billions) FROM department
SELECT max(capacity) ,  avg(capacity) ,  building FROM classroom GROUP BY building
SELECT max(charge_amount) FROM Charges
SELECT max(created) FROM votes WHERE state  =  'CA'
SELECT max(customer_code) ,  min(customer_code) FROM Customers
SELECT max(date_moved_in) FROM Residents
SELECT max(elevation) FROM airports WHERE country  =  'Iceland'
SELECT max(followers) ,  sum(followers) FROM user_profiles
SELECT max(gradepoint) ,  min(gradepoint) FROM GRADECONVERSION
SELECT max(group_equity_shareholding) FROM operate_company
SELECT max(height) ,  avg(height) FROM mountain
SELECT max(home_games) ,  min(home_games) ,  avg(home_games) FROM stadium
SELECT max(killed) ,  min(killed) FROM death
SELECT max(market_share) ,  min(market_share) ,  avg(market_share) FROM browser
SELECT max(monthly_rental) ,  min(monthly_rental) FROM Student_Addresses
SELECT max(num_of_shops) ,  avg(Num_of_Factories) FROM manufacturer WHERE open_year  <  1990
SELECT max(product_price) ,  min(product_price) ,  product_type_code FROM products GROUP BY product_type_code ORDER BY product_type_code
SELECT max(settlement_amount) ,  min(settlement_amount) FROM settlements
SELECT max(share_count) FROM TRANSACTIONS WHERE amount_of_transaction  <  10000
SELECT max(stars) ,  min(stars) FROM Rating
SELECT max(stu_gpa) ,  avg(stu_gpa) ,  min(stu_gpa) ,  dept_code FROM student GROUP BY dept_code
SELECT max(support_rate) ,  min(consider_rate) ,  min(oppose_rate) FROM candidate
SELECT max(t1.low_temperature) ,  avg(t1.precipitation) FROM weekly_weather AS t1 JOIN station AS t2 ON t1.station_id  =  t2.id WHERE t2.network_name  =  \"Amersham\"
SELECT max(t2.active_to_date) FROM customers AS t1 JOIN customer_contact_channels AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.customer_name  =  \"Tillman Ernser\"
SELECT max(weight) ,  min(weight) FROM Player
SELECT max(weight) ,  petType FROM pets GROUP BY petType
SELECT max(years_working) ,  min(years_working) ,  avg(years_working) FROM school_bus
SELECT max_page_size FROM product GROUP BY max_page_size HAVING count(*)  >  3
SELECT max_page_size FROM product GROUP BY max_page_size ORDER BY count(*) DESC LIMIT 1
SELECT member_name FROM member EXCEPT SELECT T1.member_name FROM member AS T1 JOIN party_events AS T2 ON T1.member_id  =  T2.member_in_charge_id
SELECT member_name FROM member WHERE party_id  =  3 INTERSECT SELECT member_name FROM member WHERE party_id  =  1
SELECT membership_card FROM member WHERE address  =  'Hartford' INTERSECT SELECT membership_card FROM member WHERE address  =  'Waterbury'
SELECT meter_200 ,  meter_300 FROM swimmer WHERE nationality  =  'Australia'
SELECT min(Crime_rate) ,  max(Crime_rate) FROM county_public_safety
SELECT min(Number_products) ,  max(Number_products) FROM shop
SELECT min(Order_Quantity) ,  avg(Order_Quantity) ,  max(Order_Quantity) FROM INVOICES
SELECT min(T1.duration) ,  min(T2.rating) ,  T2.genre_is FROM files AS T1 JOIN song AS T2 ON T1.f_id  =  T2.f_id GROUP BY T2.genre_is ORDER BY T2.genre_is
SELECT min(T2.HS) ,   T1.pPos FROM tryout AS T1 JOIN player AS T2 ON T1.pID  =  T2.pID GROUP BY T1.pPos
SELECT min(Version_Number) ,  template_type_code FROM Templates
SELECT min(Vote_Percent) ,  max(Vote_Percent) FROM election
SELECT min(age) ,  avg(age) ,  max(age) FROM Student
SELECT min(age) ,  job FROM Person GROUP BY job
SELECT min(age) ,  max(age) ,  avg(age) FROM people
SELECT min(amount_of_transaction) FROM TRANSACTIONS WHERE transaction_type_code  =  \"PUR\" AND share_count  >  50
SELECT min(bathroom_count) ,  max(bathroom_count) FROM Apartments
SELECT min(distance) ,  avg(distance) ,  max(distance) FROM Aircraft
SELECT min(grade) FROM Highschooler WHERE id NOT IN (SELECT T1.student_id FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id)
SELECT min(loser_rank) FROM matches
SELECT min(low_temperature) ,  max(wind_speed_mph) FROM weekly_weather
SELECT min(market_value) ,  max(market_value) ,  avg(market_value) FROM company
SELECT min(membership_amount) ,  max(membership_amount) FROM branch WHERE open_year  =  2011 OR city  =  'London'
SELECT min(product_price) ,  max(product_price) ,  avg(product_price) FROM products
SELECT min(rank) FROM review
SELECT min(salary) ,  dept_name FROM instructor GROUP BY dept_name HAVING avg(salary)  >  (SELECT avg(salary) FROM instructor)
SELECT min(seating) ,  max(seating) ,  avg(seating) FROM track
SELECT min(snatch) FROM body_builder
SELECT minister ,  party_name FROM party ORDER BY took_office DESC
SELECT minister ,  took_office ,  left_office FROM party ORDER BY left_office
SELECT minister FROM party ORDER BY left_office DESC LIMIT 1
SELECT minister FROM party WHERE party_name != 'Progress Party'
SELECT minister FROM party WHERE took_office  >  1961 OR took_office  <  1959
SELECT model_name FROM chip_model EXCEPT SELECT chip_model FROM phone WHERE Accreditation_type  =  'Full'
SELECT model_name FROM chip_model ORDER BY launch_year
SELECT museum_id ,  name FROM museum ORDER BY num_of_staff DESC LIMIT 1
SELECT name ,  Assets_billion FROM Companies ORDER BY name ASC
SELECT name ,  CLASS ,  date FROM race
SELECT name ,  CLASS ,  rank FROM captain
SELECT name ,  CLASS FROM ship WHERE ship_id NOT IN (SELECT ship_id FROM captain)
SELECT name ,  LOCATION ,  district FROM shop ORDER BY number_products DESC
SELECT name ,  LOCATION ,  number_of_platforms FROM station
SELECT name ,  LOCATION ,  seating FROM track ORDER BY year_opened DESC LIMIT 1
SELECT name ,  LOCATION ,  year_opened FROM track WHERE seating  >  (SELECT avg(seating) FROM track)
SELECT name ,  LOCATION FROM cinema ORDER BY capacity DESC LIMIT 1
SELECT name ,  LOCATION FROM cinema WHERE capacity  >  (SELECT avg(capacity) FROM cinema)
SELECT name ,  LOCATION FROM enzyme
SELECT name ,  LOCATION FROM station ORDER BY Annual_entry_exit ,  Annual_interchanges
SELECT name ,  LOCATION FROM track
SELECT name ,  Level_of_membership FROM visitor WHERE Level_of_membership  >  4 ORDER BY age DESC
SELECT name ,  Num_of_Component FROM furniture ORDER BY market_rate LIMIT 1
SELECT name ,  RESULT ,  bulgarian_commander FROM battle EXCEPT SELECT T1.name ,  T1.result ,  T1.bulgarian_commander FROM battle AS T1 JOIN ship AS T2 ON T1.id  =  T2.lost_in_battle WHERE T2.location  =  'English Channel'
SELECT name ,  RESULT FROM battle WHERE bulgarian_commander != 'Boril'
SELECT name ,  TIME ,  service FROM train
SELECT name ,  TYPE ,  flag FROM ship ORDER BY built_year DESC LIMIT 1
SELECT name ,  address_road ,  city FROM branch ORDER BY open_year
SELECT name ,  age ,  country FROM artist ORDER BY Year_Join
SELECT name ,  age ,  weight FROM Dogs WHERE abandoned_yn  =  1
SELECT name ,  age FROM people WHERE is_male  =  'T' AND people_id NOT IN (SELECT male_id FROM wedding)
SELECT name ,  average_attendance ,  total_attendance FROM stadium EXCEPT SELECT T2.name ,  T2.average_attendance ,  T2.total_attendance FROM game AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.id JOIN injury_accident AS T3 ON T1.id  =  T3.game_id
SELECT name ,  born_state ,  age FROM head ORDER BY age
SELECT name ,  built_year FROM mill WHERE TYPE  =  'Grondzeiler'
SELECT name ,  capacity FROM stadium ORDER BY average DESC LIMIT 1
SELECT name ,  city ,  country ,  elevation FROM airports WHERE city  =  'New York'
SELECT name ,  city ,  country FROM airports ORDER BY elevation DESC LIMIT 1
SELECT name ,  city ,  country FROM airports ORDER BY elevation LIMIT 1
SELECT name ,  city FROM branch WHERE branch_id NOT IN (SELECT branch_id FROM membership_register_branch)
SELECT name ,  cost FROM procedures ORDER BY cost DESC
SELECT name ,  country ,  age FROM singer ORDER BY age DESC
SELECT name ,  country FROM people WHERE age  <  (SELECT avg(age) FROM people)
SELECT name ,  country FROM singer WHERE song_name LIKE '%Hey%'
SELECT name ,  course_id FROM instructor AS T1 JOIN teaches AS T2 ON T1.ID  =  T2.ID
SELECT name ,  course_id FROM instructor AS T1 JOIN teaches AS T2 ON T1.ID  =  T2.ID WHERE T1.dept_name  =  'Art'
SELECT name ,  damage_millions_USD FROM storm ORDER BY max_speed DESC
SELECT name ,  date FROM battle
SELECT name ,  date FROM races ORDER BY date DESC LIMIT 1
SELECT name ,  dates_active ,  number_deaths FROM storm WHERE number_deaths  >=  1
SELECT name ,  dept_name FROM student ORDER BY tot_cred
SELECT name ,  distance FROM Aircraft
SELECT name ,  distance FROM Aircraft WHERE aid  =  12
SELECT name ,  email FROM user_profiles ORDER BY followers LIMIT 1
SELECT name ,  email FROM user_profiles WHERE followers  >  1000
SELECT name ,  email FROM user_profiles WHERE name LIKE '%Swift%'
SELECT name ,  flag FROM ship WHERE ship_id NOT IN (SELECT ship_id FROM captain WHERE rank  =  'Midshipman')
SELECT name ,  followers FROM user_profiles ORDER BY followers DESC
SELECT name ,  furniture_id FROM furniture ORDER BY market_rate DESC LIMIT 1
SELECT name ,  grade FROM Highschooler
SELECT name ,  headquarter ,  founder FROM manufacturers ORDER BY revenue DESC LIMIT 1
SELECT name ,  headquarter ,  revenue FROM manufacturers ORDER BY revenue DESC
SELECT name ,  headquarter FROM manufacturers ORDER BY revenue DESC
SELECT name ,  height ,  prominence FROM mountain WHERE range != 'Aberdare Range'
SELECT name ,  home_city ,  age FROM driver
SELECT name ,  job FROM Person ORDER BY name
SELECT name ,  lat ,  city FROM station ORDER BY lat LIMIT 1
SELECT name ,  main_services FROM station ORDER BY total_passengers DESC LIMIT 3
SELECT name ,  manufacturer_id FROM manufacturer ORDER BY open_year
SELECT name ,  max(revenue) ,  Headquarter FROM manufacturers GROUP BY Headquarter
SELECT name ,  member_id FROM member WHERE Membership_card  =  'Black' OR age  <  30
SELECT name ,  nationality ,  id FROM architect WHERE gender  =  'male' ORDER BY name
SELECT name ,  open_date ,  organized_by FROM Church
SELECT name ,  open_date FROM church ORDER BY open_date DESC LIMIT 3
SELECT name ,  open_year FROM manufacturer WHERE num_of_shops  >  10 OR Num_of_Factories  <  10
SELECT name ,  openning_year ,  capacity FROM cinema
SELECT name ,  openning_year FROM cinema ORDER BY openning_year DESC
SELECT name ,  operating_system FROM web_client_accelerator EXCEPT SELECT T1.name ,  T1.operating_system FROM web_client_accelerator AS T1 JOIN accelerator_compatible_browser AS T2 ON T2.accelerator_id  =  T1.id JOIN browser AS T3 ON T2.browser_id  =  T3.id WHERE T3.name  =  'Opera'
SELECT name ,  operating_system FROM web_client_accelerator WHERE CONNECTION != 'Broadband'
SELECT name ,  origin ,  OWNER FROM program
SELECT name ,  phone FROM appointment AS T1 JOIN patient AS T2 ON T1.patient  =  T2.ssn GROUP BY T1.patient HAVING count(*)  >  1
SELECT name ,  price FROM Products ORDER BY price ASC LIMIT 1
SELECT name ,  price FROM products
SELECT name ,  price FROM products WHERE price  >=  180 ORDER BY price DESC ,  name ASC
SELECT name ,  prominence FROM mountain EXCEPT SELECT T1.name ,  T1.prominence FROM mountain AS T1 JOIN photos AS T2 ON T1.id  =  T2.mountain_id JOIN camera_lens AS T3 ON T2.camera_lens_id  =  T3.id WHERE T3.brand  =  'Sigma'
SELECT name ,  salary FROM Employee ORDER BY salary
SELECT name ,  salary FROM Employee WHERE eid  =  242518965
SELECT name ,  salary FROM instructor WHERE salary  <  (SELECT avg(salary) FROM instructor WHERE dept_name  =  'Physics')
SELECT name ,  seating FROM track WHERE year_opened  >  2000 ORDER BY seating
SELECT name ,  service FROM train ORDER BY TIME
SELECT name ,  street_address ,  floors FROM building ORDER BY floors
SELECT name ,  tonnage FROM ship ORDER BY name DESC
SELECT name ,  total_passengers FROM station WHERE LOCATION != 'London'
SELECT name ,  trade_name FROM medicine EXCEPT SELECT T1.name ,  T1.trade_name FROM medicine AS T1 JOIN medicine_enzyme_interaction AS T2 ON T2.medicine_id  =  T1.id JOIN enzyme AS T3 ON T3.id  =  T2.enzyme_id WHERE T3.product  =  'Protoporphyrinogen IX'
SELECT name ,  trade_name FROM medicine WHERE FDA_approved  =  'Yes'
SELECT name ,  year_join FROM artist WHERE country != 'United States'
SELECT name FROM Aircraft ORDER BY distance LIMIT 3
SELECT name FROM Aircraft WHERE distance  >  (SELECT avg(distance) FROM Aircraft)
SELECT name FROM CLub WHERE Club_ID NOT IN (SELECT Club_ID FROM player)
SELECT name FROM Companies ORDER BY Market_Value_billion DESC
SELECT name FROM Companies WHERE Headquarters != 'USA'
SELECT name FROM Employee EXCEPT SELECT T1.name FROM Employee AS T1 JOIN Certificate AS T2 ON T1.eid  =  T2.eid JOIN Aircraft AS T3 ON T3.aid  =  T2.aid WHERE T3.name  =  \"Boeing 737-800\"
SELECT name FROM Employee ORDER BY salary ASC LIMIT 3
SELECT name FROM Employee WHERE salary  >  (SELECT avg(salary) FROM Employee)
SELECT name FROM Highschooler EXCEPT SELECT T2.name FROM Friend AS T1 JOIN Highschooler AS T2 ON T1.student_id  =  T2.id
SELECT name FROM Highschooler WHERE grade  =  10
SELECT name FROM Person ORDER BY age DESC
SELECT name FROM Person WHERE age  <  30
SELECT name FROM Person WHERE age  =  (SELECT max(age) FROM person)
SELECT name FROM Person WHERE age  >  (SELECT min(age) FROM person WHERE job  =  'engineer') ORDER BY age
SELECT name FROM Person WHERE gender  =  'male' AND age  =  (SELECT min(age) FROM person WHERE gender  =  'male' )
SELECT name FROM Person WHERE gender  =  'male' ORDER BY age
SELECT name FROM Person WHERE job  =  'student' AND age  =  (SELECT max(age) FROM person WHERE job  =  'student' )
SELECT name FROM PersonFriend GROUP BY name HAVING count(*)  =  1
SELECT name FROM PersonFriend WHERE friend =  'Alice' AND YEAR  =  (SELECT min(YEAR) FROM PersonFriend WHERE friend =  'Alice')
SELECT name FROM Reviewer UNION SELECT title FROM Movie
SELECT name FROM accounts ORDER BY name
SELECT name FROM airport WHERE country != 'Iceland'
SELECT name FROM airport WHERE name LIKE '%international%'
SELECT name FROM airports WHERE city  =  'Goroka'
SELECT name FROM airports WHERE country  =  'Cuba' OR country  =  'Argentina'
SELECT name FROM airports WHERE elevation BETWEEN -50 AND 50
SELECT name FROM appointment AS T1 JOIN patient AS T2 ON T1.patient  =  T2.ssn
SELECT name FROM artist EXCEPT SELECT T2.name FROM exhibition AS T1 JOIN artist AS T2 ON T1.artist_id  =  T2.artist_id WHERE T1.year  =  2004
SELECT name FROM artist ORDER BY year_join DESC LIMIT 1
SELECT name FROM artist WHERE artist_id NOT IN (SELECT artist_id FROM exhibition)
SELECT name FROM battle WHERE bulgarian_commander  =  'Kaloyan' AND latin_commander  =  'Baldwin I'
SELECT name FROM branch ORDER BY membership_amount DESC LIMIT 3
SELECT name FROM browser ORDER BY market_share DESC
SELECT name FROM building EXCEPT SELECT T1.name FROM building AS T1 JOIN institution AS T2 ON T1.building_id  =  T2.building_id WHERE T2.founded  =  2003
SELECT name FROM building ORDER BY height_feet DESC LIMIT 1
SELECT name FROM building WHERE building_id NOT IN (SELECT building_id FROM institution)
SELECT name FROM building WHERE height_feet  >=  200 AND floors  >=  20
SELECT name FROM buildings ORDER BY Height DESC
SELECT name FROM buildings WHERE Status  =  \"on-hold\" ORDER BY Stories ASC
SELECT name FROM buildings WHERE id NOT IN (SELECT building_id FROM Office_locations)
SELECT name FROM camera_lens WHERE name LIKE \"%Digital%\"
SELECT name FROM captain ORDER BY age DESC
SELECT name FROM captain ORDER BY age LIMIT 1
SELECT name FROM captain WHERE rank  =  'Midshipman' OR rank  =  'Lieutenant'
SELECT name FROM channel ORDER BY rating_in_percent DESC
SELECT name FROM channel WHERE OWNER  =  'CCTV' OR OWNER  =  'HBS'
SELECT name FROM channel WHERE OWNER != 'CCTV'
SELECT name FROM church EXCEPT SELECT T1.name FROM church AS T1 JOIN wedding AS T2 ON T1.church_id  =  T2.church_id WHERE T2.year  =  2015
SELECT name FROM church ORDER BY open_date DESC
SELECT name FROM circuits WHERE country = \"UK\" OR country = \"Malaysia\"
SELECT name FROM city WHERE Population BETWEEN 160000 AND 900000
SELECT name FROM city WHERE county_ID  =  (SELECT county_ID FROM county_public_safety ORDER BY Police_officers DESC LIMIT 1)
SELECT name FROM city WHERE county_id IN (SELECT county_id FROM county_public_safety WHERE Crime_rate  <  100)
SELECT name FROM club ORDER BY Start_year ASC
SELECT name FROM club ORDER BY name DESC
SELECT name FROM company WHERE Sales_in_Billion  >  200 ORDER BY Sales_in_Billion ,  Profits_in_Billion DESC
SELECT name FROM driver ORDER BY age DESC
SELECT name FROM driver WHERE driver_id NOT IN (SELECT driver_id FROM school_bus)
SELECT name FROM driver WHERE home_city  =  'Hartford' AND age  <  40
SELECT name FROM employee ORDER BY age
SELECT name FROM employee WHERE Employee_ID NOT IN (SELECT Employee_ID FROM evaluation)
SELECT name FROM enzyme ORDER BY name DESC
SELECT name FROM enzyme WHERE name LIKE \"%ALA%\"
SELECT name FROM enzyme WHERE product != 'Heme'
SELECT name FROM event ORDER BY YEAR DESC
SELECT name FROM event ORDER BY YEAR DESC LIMIT 1
SELECT name FROM head WHERE born_state != 'California'
SELECT name FROM instructor ORDER BY salary DESC LIMIT 1
SELECT name FROM instructor WHERE dept_name  =  'Comp. Sci.'
SELECT name FROM instructor WHERE dept_name  =  'Comp. Sci.'  AND salary  >  80000
SELECT name FROM instructor WHERE dept_name  =  'Statistics' ORDER BY salary LIMIT 1
SELECT name FROM instructor WHERE id NOT IN (SELECT id FROM teaches WHERE semester  =  'Spring')
SELECT name FROM instructor WHERE id NOT IN (SELECT id FROM teaches)
SELECT name FROM instructor WHERE name LIKE '%dar%'
SELECT name FROM instructor WHERE salary  >  (SELECT max(salary) FROM instructor WHERE dept_name  =  'Biology')
SELECT name FROM instructor WHERE salary  >  (SELECT min(salary) FROM instructor WHERE dept_name  =  'Biology')
SELECT name FROM manufacturers EXCEPT SELECT T2.name FROM products AS T1 JOIN manufacturers AS T2 ON T1.Manufacturer  =  T2.code WHERE T1.name  =  'DVD drive'
SELECT name FROM manufacturers WHERE revenue  <  (SELECT min(revenue) FROM manufacturers WHERE headquarter  =  'Austin')
SELECT name FROM manufacturers WHERE revenue  >  (SELECT avg(revenue) FROM manufacturers)
SELECT name FROM manufacturers WHERE revenue BETWEEN 100 AND 150
SELECT name FROM member WHERE address  =  'Harford' OR address  =  'Waterbury'
SELECT name FROM member WHERE member_id NOT IN (SELECT member_id FROM membership_register_branch)
SELECT name FROM mill WHERE LOCATION != 'Donceel'
SELECT name FROM mill WHERE name LIKE '%Moulin%'
SELECT name FROM museum WHERE Museum_ID NOT IN (SELECT museum_id FROM visit)
SELECT name FROM museum WHERE num_of_staff  >  (SELECT min(num_of_staff) FROM museum WHERE open_year  >  2010)
SELECT name FROM patient EXCEPT SELECT T1.name FROM patient AS T1 JOIN Prescribes AS T2 ON T2.Patient  =  T1.SSN JOIN Medication AS T3 ON T2.Medication  =  T3.Code WHERE T3.name  =  'Procrastin-X'
SELECT name FROM people ORDER BY date_of_birth
SELECT name FROM people WHERE height  <  (SELECT avg(height) FROM people)
SELECT name FROM people WHERE height  >  200 OR height  <  190
SELECT name FROM people WHERE people_id NOT IN (SELECT people_id FROM candidate)
SELECT name FROM person EXCEPT SELECT T2.name FROM Person AS T1 JOIN PersonFriend AS T2 ON T1.name  =  T2.friend WHERE T1.job  =  'student'
SELECT name FROM person EXCEPT SELECT name FROM PersonFriend
SELECT name FROM physician EXCEPT SELECT T2.name FROM appointment AS T1 JOIN physician AS T2 ON T1.Physician  =  T2.EmployeeID
SELECT name FROM physician WHERE POSITION LIKE '%senior%'
SELECT name FROM pilot WHERE pilot_id NOT IN (SELECT Winning_Pilot  FROM MATCH WHERE country  =  'Australia')
SELECT name FROM player WHERE points  >  (SELECT avg(points) FROM player)
SELECT name FROM procedures ORDER BY cost LIMIT 3
SELECT name FROM procedures WHERE cost  <  5000 INTERSECT SELECT T3.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T1.name  =  \"John Wen\"
SELECT name FROM procedures WHERE cost  >  1000 EXCEPT SELECT T3.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T1.name  =  \"John Wen\"
SELECT name FROM procedures WHERE cost  >  1000 UNION SELECT T3.name FROM physician AS T1 JOIN trained_in AS T2 ON T1.employeeid  =  T2.physician JOIN procedures AS T3 ON T3.code  =  T2.treatment WHERE T1.name  =  \"John Wen\"
SELECT name FROM products WHERE price  <=  200
SELECT name FROM program EXCEPT SELECT t1.name FROM program AS t1 JOIN broadcast AS t2 ON t1.program_id  =  t2.program_id WHERE t2.Time_of_day  =  \"Morning\"
SELECT name FROM program ORDER BY launch
SELECT name FROM program ORDER BY launch DESC LIMIT 1
SELECT name FROM program WHERE origin != 'Beijing'
SELECT name FROM projects ORDER BY hours DESC LIMIT 1
SELECT name FROM projects WHERE hours  >  (SELECT avg(hours) FROM projects)
SELECT name FROM projects WHERE hours BETWEEN 100 AND 300
SELECT name FROM races ORDER BY date DESC LIMIT 1
SELECT name FROM races WHERE TIME > \"12:00:00\" OR TIME < \"09:00:00\"
SELECT name FROM races WHERE YEAR = 2017
SELECT name FROM races WHERE YEAR BETWEEN 2009 AND 2011
SELECT name FROM scientists EXCEPT SELECT T3.name FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T2.hours  =  (SELECT max(hours) FROM projects)
SELECT name FROM scientists ORDER BY name
SELECT name FROM ship ORDER BY built_year ,  CLASS
SELECT name FROM shop WHERE number_products  >  (SELECT avg(number_products) FROM shop)
SELECT name FROM shop WHERE shop_id NOT IN (SELECT shop_id FROM hiring)
SELECT name FROM stadium EXCEPT SELECT T2.name FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id WHERE T1.year  =  2014
SELECT name FROM stadium ORDER BY capacity DESC LIMIT 1
SELECT name FROM stadium WHERE capacity  <  (SELECT avg(capacity) FROM stadium)
SELECT name FROM stadium WHERE id NOT IN (SELECT stadium_id FROM event)
SELECT name FROM stadium WHERE name LIKE \"%Bank%\"
SELECT name FROM stadium WHERE stadium_id NOT IN (SELECT stadium_id FROM concert)
SELECT name FROM station WHERE city  =  \"Palo Alto\" EXCEPT SELECT end_station_name FROM trip GROUP BY end_station_name HAVING count(*)  >  100
SELECT name FROM station WHERE lat  <  37.5
SELECT name FROM station WHERE station_id NOT IN (SELECT station_id FROM train_station)
SELECT name FROM storm EXCEPT SELECT T1.name FROM storm AS T1 JOIN affected_region AS T2 ON T1.storm_id  =  T2.storm_id GROUP BY T1.storm_id HAVING count(*)  >=  2
SELECT name FROM storm WHERE storm_id NOT IN (SELECT storm_id FROM affected_region)
SELECT name FROM student WHERE dept_name  =  'History' ORDER BY tot_cred DESC LIMIT 1
SELECT name FROM student WHERE id IN (SELECT id FROM takes WHERE semester  =  'Fall' AND YEAR  =  2003)
SELECT name FROM student WHERE id NOT IN (SELECT T1.id FROM takes AS T1 JOIN course AS T2 ON T1.course_id  =  T2.course_id WHERE T2.dept_name  =  'Biology')
SELECT name FROM swimmer ORDER BY meter_100
SELECT name FROM swimmer WHERE id NOT IN (SELECT swimmer_id FROM record)
SELECT name FROM track EXCEPT SELECT T2.name FROM race AS T1 JOIN track AS T2 ON T1.track_id  =  T2.track_id WHERE T1.class  =  'GT'
SELECT name FROM track WHERE track_id NOT IN (SELECT track_id FROM race)
SELECT name FROM user_profiles ORDER BY followers DESC LIMIT 1
SELECT name FROM user_profiles ORDER BY followers DESC LIMIT 5
SELECT name FROM user_profiles WHERE email LIKE '%superstar%' OR email LIKE '%edu%'
SELECT name FROM useracct WHERE u_id NOT IN (SELECT u_id FROM review)
SELECT name FROM visitor WHERE Level_of_membership  >  4 ORDER BY Level_of_membership DESC
SELECT name FROM web_client_accelerator WHERE name LIKE \"%Opera%\"
SELECT name_first ,  name_last FROM player AS T1 JOIN all_star AS T2 ON T1.player_id  =  T2.player_id WHERE YEAR  =  1998
SELECT name_first ,  name_last FROM player WHERE weight  >  220 OR height  <  75
SELECT nationality ,  count(*) FROM swimmer GROUP BY nationality HAVING count(*)  >  1
SELECT note FROM death WHERE note LIKE '%East%'
SELECT nurse FROM on_call WHERE blockfloor  =  1 AND blockcode  =  1
SELECT open_date ,  count(*) FROM church GROUP BY open_date
SELECT open_date FROM church GROUP BY open_date HAVING count(*)  >=  2
SELECT open_year ,  name FROM manufacturer ORDER BY num_of_shops DESC LIMIT 1
SELECT open_year FROM branch GROUP BY open_year HAVING count(*)  >=  2
SELECT order_id ,  count(*) FROM Invoices GROUP BY order_id
SELECT order_id ,  count(*) FROM Order_items GROUP BY order_id
SELECT order_id ,  count(DISTINCT product_id) FROM Order_items GROUP BY order_id
SELECT order_id ,  customer_id FROM customer_orders WHERE order_status_code  =   \"Cancelled\" ORDER BY order_date
SELECT order_id ,  customer_id FROM orders ORDER BY date_order_placed LIMIT 1
SELECT order_id ,  sum(product_quantity) FROM Order_items GROUP BY order_id
SELECT order_id FROM orders ORDER BY date_order_placed DESC LIMIT 1
SELECT order_id FROM shipments WHERE shipment_date  =  (SELECT max(shipment_date) FROM shipments)
SELECT order_id FROM shipments WHERE shipment_date > \"2000-01-01\"
SELECT order_id FROM shipments WHERE shipment_tracking_number = \"3452\"
SELECT order_item_id FROM order_items WHERE product_id = 11
SELECT order_shipping_charges ,  customer_id FROM customer_orders WHERE order_status_code  =  'Cancelled' OR order_status_code  =  'Paid'
SELECT organisation_details FROM Organisations AS T1 JOIN organisation_Types AS T2 ON T1.organisation_type  =  T2.organisation_type WHERE T2.organisation_type_description  =  'Sponsor' ORDER BY organisation_details
SELECT organization_id FROM organizations EXCEPT SELECT parent_organization_id FROM organizations
SELECT organization_name FROM organizations ORDER BY date_formed ASC
SELECT organization_name FROM organizations ORDER BY date_formed DESC LIMIT 1
SELECT organization_name FROM organizations WHERE organization_name LIKE \"%Party%\"
SELECT organized_by ,  name FROM church WHERE open_date BETWEEN 1830 AND 1840
SELECT origin ,  count(*) FROM Flight GROUP BY origin
SELECT origin ,  count(*) FROM train GROUP BY origin
SELECT origin ,  destination FROM Flight WHERE price  >  300
SELECT origin FROM Flight GROUP BY origin ORDER BY count(*) DESC LIMIT 1
SELECT origin FROM Flight WHERE destination  =  \"Honolulu\"
SELECT origin FROM program ORDER BY origin
SELECT origin FROM train GROUP BY origin HAVING count(*)  >  1
SELECT other_account_details FROM Accounts WHERE account_name  =  \"338\"
SELECT other_details FROM Residents WHERE other_details LIKE '%Miss%'
SELECT other_hotel_details ,  star_rating_code FROM HOTELS ORDER BY price_range ASC LIMIT 3
SELECT other_hotel_details FROM HOTELS ORDER BY price_range DESC LIMIT 3
SELECT other_student_details FROM Students ORDER BY other_student_details DESC
SELECT outcome_code ,  count(*) FROM mailshot_customers GROUP BY outcome_code
SELECT pName ,  HS FROM Player WHERE HS  <  1500
SELECT pName FROM Player WHERE HS BETWEEN 500 AND 1500
SELECT pName FROM Player WHERE yCard  =  'yes' ORDER BY HS DESC
SELECT pPos FROM tryout GROUP BY pPos ORDER BY count(*) DESC LIMIT 1
SELECT package_option ,  series_name FROM TV_Channel WHERE hight_definition_TV  =  \"yes\"
SELECT package_option FROM TV_Channel WHERE id NOT IN (SELECT channel FROM cartoon WHERE directed_by  =  'Ben Jones')
SELECT part_id ,  chargeable_amount FROM Parts ORDER BY chargeable_amount ASC LIMIT 1
SELECT participant_type_code FROM participants GROUP BY participant_type_code ORDER BY count(*) DESC LIMIT 1
SELECT partitionid FROM user_profiles WHERE name  =  'Iron Man'
SELECT party ,  count(*) FROM driver GROUP BY party
SELECT party FROM driver WHERE home_city  =  'Hartford' AND age  >  40
SELECT party_name FROM party WHERE party_id NOT IN (SELECT party_id FROM Member)
SELECT patient FROM stay WHERE room  =  111 ORDER BY staystart DESC LIMIT 1
SELECT patient FROM undergoes ORDER BY dateundergoes LIMIT 1
SELECT payment_date FROM payment ORDER BY payment_date ASC LIMIT 1
SELECT payment_date FROM payment WHERE amount  >  10 UNION SELECT T1.payment_date FROM payment AS T1 JOIN staff AS T2 ON T1.staff_id  =  T2.staff_id WHERE T2.first_name  =  'Elsa'
SELECT payment_date FROM payments WHERE amount_paid  >  300 OR payment_type_code  =  'Check'
SELECT payment_method FROM Customers GROUP BY payment_method ORDER BY count(*) DESC LIMIT 1
SELECT payment_method FROM customers GROUP BY payment_method ORDER BY count(*) DESC LIMIT 1
SELECT payment_method_code ,  count(*) FROM INVOICES GROUP BY payment_method_code
SELECT payment_method_code ,  count(*) FROM customers GROUP BY payment_method_code
SELECT payment_method_code ,  party_phone FROM parties WHERE party_email  =  \"enrico09@example.com\"
SELECT payment_method_code FROM INVOICES GROUP BY payment_method_code ORDER BY count(*) DESC LIMIT 1
SELECT payment_method_code FROM parties GROUP BY payment_method_code HAVING count(*)  >  3
SELECT personal_name ,  family_name FROM Students ORDER BY family_name
SELECT personal_name FROM Course_Authors_and_Tutors INTERSECT SELECT personal_name FROM Students
SELECT personal_name FROM Students EXCEPT SELECT T1.personal_name FROM Students AS T1 JOIN Student_Course_Enrolment AS T2 ON T1.student_id  =  T2.student_id
SELECT petid ,  weight FROM pets WHERE pet_age  >  1
SELECT pets_allowed_yn ,  avg(price_range) FROM HOTELS GROUP BY pets_allowed_yn
SELECT pettype ,  weight FROM pets ORDER BY pet_age LIMIT 1
SELECT phone ,  postal_code FROM address WHERE address  =  '1031 Daugavpils Parkway'
SELECT phone ,  room ,  building FROM Faculty WHERE Fname  =  \"Jerry\" AND Lname  =  \"Prince\"
SELECT phone_number FROM customers UNION SELECT phone_number FROM staff
SELECT phone_number FROM employees WHERE salary BETWEEN 8000 AND 12000
SELECT physician ,  department FROM affiliated_with WHERE primaryaffiliation  =  1
SELECT pilot FROM flight GROUP BY pilot ORDER BY count(*) DESC LIMIT 1
SELECT player ,  number_of_matches ,  SOURCE FROM injury_accident WHERE injury != 'Knee problem'
SELECT player FROM match_season ORDER BY College ASC
SELECT player_api_id FROM Player WHERE height  >=  180 AND height  <=  190 INTERSECT SELECT player_api_id FROM Player_Attributes WHERE preferred_foot  =  \"left\"
SELECT player_api_id FROM Player WHERE height  >=  180 INTERSECT SELECT player_api_id FROM Player_Attributes WHERE overall_rating  >  85
SELECT policy_type_code ,  count(*) FROM policies GROUP BY policy_type_code
SELECT policy_type_code FROM available_policies GROUP BY policy_type_code HAVING count(*)  >  4
SELECT policy_type_code FROM available_policies GROUP BY policy_type_code ORDER BY count(*) DESC LIMIT 1
SELECT policy_type_code FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id WHERE t2.customer_details  =  \"Dayana Robel\"
SELECT policy_type_code FROM policies GROUP BY policy_type_code HAVING count(*)  >  2
SELECT policy_type_code FROM policies GROUP BY policy_type_code ORDER BY count(*) DESC LIMIT 1
SELECT poll_source FROM candidate GROUP BY poll_source ORDER BY count(*) DESC LIMIT 1
SELECT poll_source FROM candidate ORDER BY oppose_rate DESC LIMIT 1
SELECT preferred_foot ,  avg(overall_rating) FROM Player_Attributes GROUP BY preferred_foot
SELECT preferred_foot ,  count(*) FROM Player_Attributes WHERE overall_rating  >  80 GROUP BY preferred_foot
SELECT premises_type ,  count(*) FROM premises GROUP BY premises_type
SELECT premises_type ,  premise_details FROM premises ORDER BY premises_type
SELECT price_range FROM HOTELS
SELECT price_range FROM HOTELS WHERE star_rating_code  =  \"5\"
SELECT problem_id FROM problems WHERE date_problem_reported < \"1978-06-26\"
SELECT problem_id FROM problems WHERE date_problem_reported > \"1978-06-26\"
SELECT problem_log_id ,  log_entry_date FROM problem_log WHERE problem_id = 10
SELECT problem_log_id ,  log_entry_description FROM problem_log
SELECT problem_log_id ,  problem_id FROM problem_log ORDER BY log_entry_date LIMIT 1
SELECT problem_log_id FROM problem_log ORDER BY log_entry_date DESC LIMIT 1
SELECT product ,  chromosome ,  porphyria FROM enzyme WHERE LOCATION  =  'Cytosol'
SELECT product FROM product WHERE max_page_size  =  \"A4\" AND pages_per_minute_color  <  5
SELECT product FROM product WHERE max_page_size  =  \"A4\" OR pages_per_minute_color  <  5
SELECT product FROM product WHERE product != (SELECT max_page_size FROM product GROUP BY max_page_size ORDER BY count(*) DESC LIMIT 1)
SELECT product FROM product WHERE product LIKE \"%Scanner%\"
SELECT product_category ,  count(*) FROM mailshot_campaigns GROUP BY product_category
SELECT product_category_code ,  typical_buying_price FROM products WHERE product_name  =  \"cumin\"
SELECT product_category_code FROM products WHERE product_name  =  \"flax\"
SELECT product_category_description ,  unit_of_measure FROM ref_product_categories WHERE product_category_code  =  \"Herbs\"
SELECT product_category_description FROM ref_product_categories WHERE product_category_code  =  \"Spices\"
SELECT product_description FROM products WHERE product_name  =  \"Chocolate\"
SELECT product_details FROM products WHERE product_details LIKE \"%Latte%\" OR product_details LIKE \"%Americano%\"
SELECT product_id ,  count(DISTINCT order_id) FROM Order_items GROUP BY product_id
SELECT product_id ,  product_name FROM products WHERE product_price  <  600 OR product_price  >  900
SELECT product_id ,  product_type_code FROM products ORDER BY product_price LIMIT 1
SELECT product_id FROM Order_Items GROUP BY product_id HAVING count(*)  >  3 UNION SELECT product_id FROM Product_Suppliers GROUP BY product_id HAVING sum(total_amount_purchased)  >  80000
SELECT product_id FROM order_items GROUP BY product_id ORDER BY count(*) DESC LIMIT 1
SELECT product_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE T2.staff_first_name = \"Christop\" AND T2.staff_last_name = \"Berge\" INTERSECT SELECT product_id FROM problems AS T1 JOIN staff AS T2 ON T1.closure_authorised_by_staff_id = T2.staff_id WHERE T2.staff_first_name = \"Ashley\" AND T2.staff_last_name = \"Medhurst\"
SELECT product_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE T2.staff_first_name = \"Dameon\" AND T2.staff_last_name = \"Frami\" UNION SELECT product_id FROM problems AS T1 JOIN staff AS T2 ON T1.reported_by_staff_id = T2.staff_id WHERE T2.staff_first_name = \"Jolie\" AND T2.staff_last_name = \"Weber\"
SELECT product_id FROM product_suppliers ORDER BY total_amount_purchased DESC LIMIT 3
SELECT product_id FROM products_booked GROUP BY product_id HAVING count(*)  =  3
SELECT product_name ,  product_category_code FROM products ORDER BY product_price DESC LIMIT 1
SELECT product_name ,  product_description FROM products_for_hire WHERE product_type_code  =  'Cutlery' AND daily_hire_cost  <  20
SELECT product_name ,  product_id FROM products WHERE product_price BETWEEN 600 AND 700
SELECT product_name ,  typical_buying_price FROM products
SELECT product_name FROM Products EXCEPT SELECT T1.product_name FROM Products AS T1 JOIN Order_items AS T2 ON T1.product_id  =  T2.product_id
SELECT product_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id JOIN ref_colors AS t4 ON t1.color_code  =  t4.color_code WHERE t4.color_description  =  \"red\" AND t3.characteristic_name  =  \"fast\"
SELECT product_name FROM products ORDER BY product_price DESC LIMIT 1
SELECT product_name FROM products WHERE product_category_code  =  \"Spices\"
SELECT product_name FROM products WHERE product_type_code  =  'Hardware' ORDER BY product_price DESC LIMIT 1
SELECT product_price FROM products WHERE product_id NOT IN (SELECT product_id FROM complaints)
SELECT product_type_code ,  avg(product_price) FROM products GROUP BY product_type_code
SELECT product_type_code ,  max(product_price) ,  min(product_price) FROM products GROUP BY product_type_code
SELECT product_type_code ,  product_name FROM products WHERE product_price  >  1000 OR product_price  <  500
SELECT product_type_code FROM products GROUP BY product_type_code HAVING avg(product_price)  >  (SELECT avg(product_price) FROM products)
SELECT product_type_code FROM products GROUP BY product_type_code HAVING count(*)  >=  2
SELECT professional_id ,  last_name ,  cell_number FROM Professionals WHERE state  =  'Indiana' UNION SELECT T1.professional_id ,  T1.last_name ,  T1.cell_number FROM Professionals AS T1 JOIN Treatments AS T2 ON T1.professional_id  =  T2.professional_id GROUP BY T1.professional_id HAVING count(*)  >  2
SELECT professional_id ,  role_code ,  email_address FROM Professionals EXCEPT SELECT T1.professional_id ,  T1.role_code ,  T1.email_address FROM Professionals AS T1 JOIN Treatments AS T2 ON T1.professional_id  =  T2.professional_id
SELECT project_details FROM Projects WHERE organisation_id IN ( SELECT organisation_id FROM Projects GROUP BY organisation_id ORDER BY count(*) DESC LIMIT 1 )
SELECT project_details FROM Projects WHERE project_id NOT IN ( SELECT project_id FROM Project_Staff WHERE role_code  =  'researcher' )
SELECT project_details FROM Projects WHERE project_id NOT IN ( SELECT project_id FROM Project_outcomes )
SELECT project_id ,  count(*) FROM Documents GROUP BY project_id
SELECT project_id ,  project_details FROM Projects
SELECT project_id FROM Documents GROUP BY project_id HAVING count(*)  >=  2
SELECT project_id FROM Documents GROUP BY project_id ORDER BY count(*) ASC LIMIT 1
SELECT property_name FROM Properties WHERE property_type_code  =  \"House\" UNION SELECT property_name FROM Properties WHERE property_type_code  =  \"Apartment\" AND room_count  >  1
SELECT publisher ,  count(*) FROM book_club GROUP BY publisher
SELECT publisher FROM book_club EXCEPT SELECT publisher FROM book_club WHERE YEAR  =  1989
SELECT publisher FROM book_club GROUP BY publisher ORDER BY count(*) DESC LIMIT 1
SELECT publisher FROM book_club WHERE YEAR  =  1989 INTERSECT SELECT publisher FROM book_club WHERE YEAR  =  1990
SELECT rID FROM Rating EXCEPT SELECT rID FROM Rating WHERE stars  =  4
SELECT rID FROM Rating WHERE stars != 4
SELECT rID FROM Reviewer WHERE name  =  \"Daniel Lewis\"
SELECT rID FROM Reviewer WHERE name LIKE \"%Mike%\"
SELECT rank ,  Fname ,  Lname FROM Faculty
SELECT rank ,  company ,  market_value FROM company WHERE main_industry  =  'Banking' ORDER BY sales_billion ,  profits_billion
SELECT rank ,  count(*) FROM Faculty GROUP BY rank
SELECT rank ,  sex ,  count(*) FROM Faculty GROUP BY rank ,  sex
SELECT rank FROM Faculty GROUP BY rank ORDER BY count(*) ASC LIMIT 1
SELECT rank FROM captain EXCEPT SELECT rank FROM captain WHERE CLASS  =  'Third-rate ship of the line'
SELECT rank FROM captain GROUP BY rank ORDER BY count(*) DESC LIMIT 1
SELECT rank FROM captain WHERE CLASS  =  'Cutter' INTERSECT SELECT rank FROM captain WHERE CLASS  =  'Armed schooner'
SELECT region_code ,  region_name FROM region ORDER BY region_code
SELECT region_name FROM region ORDER BY region_name
SELECT region_name FROM region WHERE region_id NOT IN (SELECT region_id FROM affected_region)
SELECT region_name FROM region WHERE region_name != 'Denmark'
SELECT rid FROM routes WHERE dst_apid IN (SELECT apid FROM airports WHERE country  =  'United States') AND src_apid IN (SELECT apid FROM airports WHERE country  =  'United States')
SELECT role_code ,  count(*) FROM Employees GROUP BY role_code
SELECT role_code ,  role_name ,  role_description FROM ROLES
SELECT role_code ,  street ,  city ,  state FROM professionals WHERE city LIKE '%West%'
SELECT role_code FROM Employees GROUP BY role_code HAVING count(*)  >=  3
SELECT role_code FROM Employees GROUP BY role_code ORDER BY count(*) ASC LIMIT 1
SELECT role_code FROM Employees GROUP BY role_code ORDER BY count(*) DESC LIMIT 1
SELECT role_code FROM Project_Staff GROUP BY role_code ORDER BY count(*) DESC LIMIT 1
SELECT role_code FROM Project_Staff WHERE date_from  >  '2003-04-19 15:06:20' AND date_to  <  '2016-03-15 00:33:18'
SELECT role_description FROM ROLES WHERE role_code  =  (SELECT role_code FROM users WHERE user_login  =  1 GROUP BY role_code ORDER BY count(*) DESC LIMIT 1)
SELECT role_description FROM ROLES WHERE role_name  =  \"Proof Reader\"
SELECT role_description FROM Staff_Roles WHERE role_code  =  'researcher'
SELECT role_name ,  role_description FROM ROLES WHERE role_code  =  \"MG\"
SELECT roomname FROM rooms WHERE baseprice BETWEEN 120 AND 150
SELECT route_name FROM Delivery_Routes ORDER BY route_name
SELECT salary ,  manager_id FROM employees WHERE manager_id != \"null\"
SELECT salary FROM salary WHERE YEAR  =  2010 UNION SELECT salary FROM salary WHERE YEAR  =  2001
SELECT sales_details FROM sales UNION SELECT purchase_details FROM purchases
SELECT school ,  LOCATION FROM university WHERE affiliation  =  'Public'
SELECT school ,  TYPE FROM school WHERE school_id NOT IN (SELECT school_id FROM school_bus)
SELECT school ,  nickname FROM university ORDER BY founded
SELECT school FROM university WHERE founded  >  1850 OR affiliation  =  'Public'
SELECT school_code FROM department WHERE dept_name  =  \"Accounting\"
SELECT school_name ,  LOCATION ,  mascot FROM school
SELECT school_name FROM school ORDER BY enrollment LIMIT 1
SELECT school_name FROM school ORDER BY school_name
SELECT school_name FROM school WHERE school_id NOT IN (SELECT school_id FROM endowment)
SELECT season ,  home_team ,  away_team FROM game
SELECT section_description FROM Sections WHERE section_name  =  'h'
SELECT section_name ,  section_description FROM Sections
SELECT section_name FROM Sections ORDER BY section_name DESC
SELECT semester ,  YEAR FROM SECTION GROUP BY semester ,  YEAR ORDER BY count(*) DESC LIMIT 1
SELECT semester ,  YEAR FROM takes GROUP BY semester ,  YEAR ORDER BY count(*) LIMIT 1
SELECT semester_name FROM Semesters WHERE semester_id NOT IN( SELECT semester_id FROM Student_Enrolment )
SELECT service_id ,  event_details FROM EVENTS
SELECT service_name FROM services EXCEPT SELECT t1.service_name FROM services AS t1 JOIN party_services AS t2 ON t1.service_id  =  t2.service_id
SELECT service_name FROM services ORDER BY service_name
SELECT service_type_code FROM services ORDER BY service_type_code
SELECT sex ,  count(*) FROM Faculty WHERE rank  =  \"AsstProf\" GROUP BY sex
SELECT shop_id ,  address FROM shop WHERE score  <  (SELECT avg(score) FROM shop)
SELECT shop_id ,  count(*) FROM happy_hour GROUP BY shop_id ORDER BY count(*) DESC LIMIT 1
SELECT song_name ,  releasedate FROM song ORDER BY releasedate DESC LIMIT 1
SELECT song_name ,  song_release_year FROM singer ORDER BY age LIMIT 1
SELECT song_name FROM singer WHERE age  >  (SELECT avg(age) FROM singer)
SELECT song_name FROM song ORDER BY resolution
SELECT song_name FROM song WHERE genre_is  =  \"modern\" OR languages  =  \"english\"
SELECT song_name FROM song WHERE languages  =  \"english\"
SELECT song_name FROM song WHERE rating  <   (SELECT min(rating) FROM song WHERE languages  =  'english')
SELECT song_name FROM song WHERE rating  <  (SELECT max(rating) FROM song WHERE genre_is  =  \"blues\")
SELECT sportname ,  count(*) FROM Sportsinfo GROUP BY sportname
SELECT sportname FROM Sportsinfo WHERE onscholarship  =  'Y' GROUP BY sportname ORDER BY count(*) DESC LIMIT 1
SELECT staff_details FROM Research_Staff ORDER BY staff_details ASC
SELECT staff_id FROM Staff_Department_Assignments WHERE date_assigned_to  <  (SELECT max(date_assigned_to) FROM Staff_Department_Assignments WHERE job_title_code  =  'Clerical Staff')
SELECT star_rating_code ,  avg(price_range) FROM HOTELS GROUP BY star_rating_code
SELECT start_station_id ,  start_station_name FROM trip GROUP BY start_station_name HAVING COUNT(*)  >=  200
SELECT start_station_name ,  end_station_name FROM trip ORDER BY id LIMIT 3
SELECT start_station_name ,  start_station_id FROM trip WHERE start_date LIKE \"8/%\" GROUP BY start_station_name ORDER BY COUNT(*) DESC LIMIT 1
SELECT state ,  acc_type ,  credit_score FROM customer WHERE no_of_loans  =  0
SELECT state FROM Owners INTERSECT SELECT state FROM Professionals
SELECT state FROM bank GROUP BY state ORDER BY sum(no_of_customers) DESC LIMIT 1
SELECT state FROM customers GROUP BY state ORDER BY count(*) LIMIT 1
SELECT state_province_county ,  count(*) FROM addresses GROUP BY state_province_county
SELECT state_province_county ,  country FROM addresses WHERE zip_postcode LIKE \"4%\"
SELECT state_province_county FROM addresses WHERE address_id NOT IN (SELECT employee_address_id FROM Employees)
SELECT state_province_county FROM addresses WHERE line_1_number_building LIKE \"%6862 Kaitlyn Knolls%\"
SELECT station_id ,  LOCATION ,  manager_name FROM gas_station ORDER BY open_year
SELECT store_id FROM customer GROUP BY store_id ORDER BY count(*) DESC LIMIT 1
SELECT store_id FROM inventory GROUP BY store_id ORDER BY count(*) DESC LIMIT 1
SELECT stu_fname ,  stu_gpa FROM student WHERE stu_gpa  <  (SELECT avg(stu_gpa) FROM student)
SELECT stu_fname ,  stu_lname ,  stu_gpa FROM student WHERE stu_gpa  >  3 ORDER BY stu_dob DESC LIMIT 1
SELECT stu_gpa ,  stu_phone ,  stu_fname FROM student ORDER BY stu_gpa DESC LIMIT 5
SELECT student_capacity ,  gender FROM dorm WHERE dorm_name LIKE '%Donor%'
SELECT student_id ,  count(*) FROM Friend GROUP BY student_id
SELECT student_id ,  count(*) FROM Likes GROUP BY student_id
SELECT student_id FROM Friend INTERSECT SELECT liked_id FROM Likes
SELECT student_id FROM student_course_attendance
SELECT student_id FROM student_course_attendance WHERE course_id  =  301
SELECT student_id FROM student_course_attendance WHERE course_id = 301 ORDER BY date_of_attendance DESC LIMIT 1
SELECT student_id FROM student_course_registrations GROUP BY student_id ORDER BY count(*) LIMIT 1
SELECT student_id FROM student_course_registrations UNION SELECT student_id FROM student_course_attendance
SELECT student_id FROM students WHERE student_id NOT IN (SELECT student_id FROM student_course_attendance)
SELECT stuid FROM student EXCEPT SELECT T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat'
SELECT subject_name FROM SUBJECTS
SELECT sum(Amount_Claimed) FROM Claims
SELECT sum(Amount_Payment) FROM Payments
SELECT sum(Amount_Settled) FROM Settlements
SELECT sum(Domestic_Passengers) FROM airport WHERE Airport_Name LIKE \"%London%\"
SELECT sum(Enrollment) ,  affiliation FROM university WHERE founded  >  1850 GROUP BY affiliation
SELECT sum(Num_of_Audience) FROM festival_detail
SELECT sum(Points) FROM player
SELECT sum(Population) ,  GovernmentForm FROM country GROUP BY GovernmentForm HAVING avg(LifeExpectancy)  >  72
SELECT sum(Population) ,  avg(LifeExpectancy) ,  Continent FROM country GROUP BY Continent HAVING avg(LifeExpectancy)  <  72
SELECT sum(Population) ,  max(GNP) FROM country WHERE Continent  =  \"Asia\"
SELECT sum(Population) FROM city WHERE District  =  \"Gelderland\"
SELECT sum(Population) FROM country WHERE Name NOT IN (SELECT T1.Name FROM country AS T1 JOIN countrylanguage AS T2 ON T1.Code  =  T2.CountryCode WHERE T2.Language  =  \"English\")
SELECT sum(Rating_in_percent) ,  OWNER FROM channel GROUP BY OWNER
SELECT sum(Share_in_percent) FROM channel WHERE OWNER  =  'CCTV'
SELECT sum(SurfaceArea) FROM country WHERE Continent  =  \"Asia\" OR Continent  =  \"Europe\"
SELECT sum(SurfaceArea) FROM country WHERE Region  =  \"Caribbean\"
SELECT sum(T1.Money_Requested) FROM entrepreneur AS T1 JOIN people AS T2 ON T1.People_ID  =  T2.People_ID WHERE T2.Height  >  1.85
SELECT sum(T1.Order_Quantity) FROM ORDER_ITEMS AS T1 JOIN Products AS T2 ON T1.Product_ID  =  T2.Product_ID WHERE T2.Product_Name  =  \"photo\"
SELECT sum(T1.amount_payment) FROM Customer_Payments AS T1 JOIN Customers AS T2 ON T1.customer_id = T2.customer_id WHERE T2.first_name = \"Carole\" AND T2.last_name = \"Bernhard\"
SELECT sum(T1.budgeted) FROM budget AS T1 JOIN school AS T2 ON T1.school_id  =  T2.school_id WHERE T2.school_name  =  'Glenn'
SELECT sum(T1.crs_credit) ,  T1.dept_code FROM course AS T1 JOIN CLASS AS T2 ON T1.crs_code  =  T2.crs_code GROUP BY T1.dept_code
SELECT sum(T1.salary) FROM salary AS T1 JOIN team AS T2 ON T1.team_id  =  T2.team_id_br WHERE T2.name  = 'Boston Red Stockings' AND T1.year  =  2010
SELECT sum(T2.Killed) FROM people AS T1 JOIN perpetrator AS T2 ON T1.People_ID  =  T2.People_ID WHERE T1.Height  >  1.84
SELECT sum(T2.amount) FROM bank AS T1 JOIN loan AS T2 ON T1.branch_id  =  T2.branch_id WHERE T1.state  =  'New York'
SELECT sum(T2.balance) ,  T1.name FROM accounts AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid GROUP BY T1.name
SELECT sum(T2.balance) FROM accounts AS T1 JOIN savings AS T2 ON T1.custid  =  T2.custid WHERE T1.name != 'Brown'
SELECT sum(T2.hours) FROM assignedto AS T1 JOIN projects AS T2 ON T1.project  =  T2.code JOIN scientists AS T3 ON T1.scientist  =  T3.SSN WHERE T3.name  =  'Michael Rogers' OR T3.name  =  'Carol Smith'
SELECT sum(T2.room_count) FROM Apartment_Buildings AS T1 JOIN Apartments AS T2 ON T1.building_id  =  T2.building_id WHERE T1.building_short_name  =  \"Columbus Square\"
SELECT sum(T2.room_count) FROM Apartment_Facilities AS T1 JOIN Apartments AS T2 ON T1.apt_id  =  T2.apt_id WHERE T1.facility_code  =  \"Gym\"
SELECT sum(acc_bal) ,  state FROM customer WHERE credit_score  >  100 GROUP BY state
SELECT sum(acc_bal) FROM customer WHERE state  =  'Utah' OR state  =  'Texas'
SELECT sum(access_count) FROM documents GROUP BY document_type_code ORDER BY count(*) DESC LIMIT 1
SELECT sum(amount) ,  T1.bname FROM bank AS T1 JOIN loan AS T2 ON T1.branch_id  =  T2.branch_id GROUP BY T1.bname
SELECT sum(amount) FROM payment
SELECT sum(amount_piad) ,  avg(amount_piad) FROM claim_headers
SELECT sum(balance) FROM checking
SELECT sum(bonus) FROM evaluation
SELECT sum(budget) FROM department WHERE dept_name  =  'Marketing' OR dept_name  =  'Finance'
SELECT sum(city_population) FROM district ORDER BY city_area DESC LIMIT 3
SELECT sum(city_population) FROM district WHERE city_area  >  (SELECT avg(city_area) FROM district)
SELECT sum(credits) ,  dept_name FROM course GROUP BY dept_name
SELECT sum(crs_credit) ,  dept_code FROM course GROUP BY dept_code
SELECT sum(duration) ,  max(duration) FROM trip WHERE bike_id  =  636
SELECT sum(enr) FROM College
SELECT sum(enr) FROM college WHERE cName NOT IN (SELECT cName FROM tryout WHERE pPos  =  \"goalie\")
SELECT sum(enrollment) ,  affiliation FROM university GROUP BY affiliation
SELECT sum(enrollment) ,  avg(enrollment) FROM school
SELECT sum(enrollment) ,  min(enrollment) FROM university
SELECT sum(faculty) FROM faculty WHERE YEAR  =  2002
SELECT sum(gamesplayed) FROM Sportsinfo
SELECT sum(gamesplayed) FROM Sportsinfo WHERE sportname  =  \"Football\" AND onscholarship  =  'Y'
SELECT sum(grant_amount) ,  organisation_id FROM Grants GROUP BY organisation_id
SELECT sum(grant_amount) FROM Grants AS T1 JOIN Organisations AS T2 ON T1.organisation_id  =  T2.organisation_id JOIN organisation_Types AS T3 ON T2.organisation_type  =  T3.organisation_type WHERE T3.organisation_type_description  =  'Research'
SELECT sum(hours) FROM projects
SELECT sum(hoursperweek) ,  sum(gamesplayed) FROM Sportsinfo AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID WHERE T2.Fname  =  \"David\" AND T2.Lname  =  \"Shieber\"
SELECT sum(hoursperweek) ,  sum(gamesplayed) FROM Sportsinfo AS T1 JOIN Student AS T2 ON T1.StuID  =  T2.StuID WHERE T2.age  <  20
SELECT sum(market_rate) FROM furniture ORDER BY market_rate DESC LIMIT 2
SELECT sum(no_of_customers) FROM bank
SELECT sum(no_of_customers) FROM bank WHERE city  =  'New York City'
SELECT sum(num_of_component) FROM furniture
SELECT sum(number_deaths) ,  sum(damage_millions_USD) FROM storm WHERE max_speed  >  (SELECT avg(max_speed) FROM storm)
SELECT sum(ranking_points) ,  T1.first_name FROM players AS T1 JOIN rankings AS T2 ON T1.player_id  =  T2.player_id GROUP BY T1.first_name
SELECT sum(revenue) ,  founder FROM manufacturers GROUP BY founder
SELECT sum(revenue) ,  name FROM manufacturers GROUP BY name
SELECT sum(revenue) FROM manufacturers WHERE Headquarter  =  'Tokyo' OR Headquarter  =  'Taiwan'
SELECT sum(revenue) FROM manufacturers WHERE headquarter  =  'Austin'
SELECT sum(revenue) FROM manufacturers WHERE revenue  >  (SELECT min(revenue) FROM manufacturers WHERE headquarter  =  'Austin')
SELECT sum(settlement_amount) ,  avg(settlement_amount) FROM settlements
SELECT sum(share_count) FROM TRANSACTIONS
SELECT sum(stu_hrs) ,  dept_code FROM student GROUP BY dept_code
SELECT sum(student_capacity) FROM dorm
SELECT sum(t1.amount_claimed) FROM claim_headers AS t1 JOIN claims_documents AS t2 ON t1.claim_header_id  =  t2.claim_id WHERE t2.created_date  =  (SELECT created_date FROM claims_documents ORDER BY created_date LIMIT 1)
SELECT sum(t1.graduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  \"San Francisco State University\"
SELECT sum(t1.undergraduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  \"San Jose State University\"
SELECT sum(t2.Total_spent) FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id WHERE t1.Level_of_membership  =  1
SELECT sum(t2.order_quantity) FROM customer_orders AS t1 JOIN order_items AS t2 ON t1.order_id  =  t2.order_id WHERE t1.order_date  <  \"2018-03-17 07:13:53\"
SELECT sum(t2.order_quantity) FROM customer_orders AS t1 JOIN order_items AS t2 ON t1.order_id  =  t2.order_id WHERE t1.order_status  =  \"Cancelled\"
SELECT sum(t3.order_quantity) FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id WHERE t1.customer_name  =  \"Rodrick Heaney\"
SELECT sum(total_pounds) FROM purchase AS T1 JOIN branch AS T2 ON T1.branch_id  =  T2.branch_id WHERE T2.city  =  'London' AND T1.year  =  2018
SELECT sum(tours) ,  ranking_date FROM rankings GROUP BY ranking_date
SELECT supplier_id FROM Product_Suppliers GROUP BY supplier_id HAVING avg(total_amount_purchased)  >  50000 OR avg(total_amount_purchased)  <  30000
SELECT support_rate FROM candidate ORDER BY support_rate DESC LIMIT 3
SELECT t1.Effective_Date FROM claims AS t1 JOIN settlements AS t2 ON t1.claim_id  =  t2.claim_id GROUP BY t1.claim_id ORDER BY sum(t2.settlement_amount) DESC LIMIT 1
SELECT t1.Primary_conference FROM university AS t1 JOIN basketball_match AS t2 ON t1.school_id  =  t2.school_id ORDER BY t2.acc_percent LIMIT 1
SELECT t1.address ,  t1.shop_id FROM shop AS t1 JOIN happy_hour AS t2 ON t1.shop_id  =  t2.shop_id WHERE MONTH  =  'May'
SELECT t1.attribute_name ,  t1.attribute_id FROM Attribute_Definitions AS t1 JOIN Catalog_Contents_Additional_Attributes AS t2 ON t1.attribute_id  =  t2.attribute_id WHERE t2.attribute_value  =  0
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  \"San Francisco State University\" AND t1.year  =  2000
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  \"San Jose State University\" AND t1.year  =  2000
SELECT t1.catalog_entry_name FROM Catalog_Contents AS t1 JOIN Catalog_Contents_Additional_Attributes AS t2 ON t1.catalog_entry_id  =  t2.catalog_entry_id WHERE t2.attribute_value  =  (SELECT attribute_value FROM Catalog_Contents_Additional_Attributes GROUP BY attribute_value ORDER BY count(*) DESC LIMIT 1)
SELECT t1.catalog_entry_name FROM Catalog_Contents AS t1 JOIN Catalog_Contents_Additional_Attributes AS t2 ON t1.catalog_entry_id  =  t2.catalog_entry_id WHERE t2.catalog_level_number  =  \"8\"
SELECT t1.catalog_name ,  t1.date_of_publication FROM catalogs AS t1 JOIN catalog_structure AS t2 ON t1.catalog_id  =  t2.catalog_id WHERE catalog_level_number  >  5
SELECT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.major  =  \"600\" GROUP BY t1.clubname ORDER BY count(*) DESC LIMIT 1
SELECT t1.clubname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t3.sex  =  \"F\" GROUP BY t1.clubname ORDER BY count(*) DESC LIMIT 1
SELECT t1.color_code ,  t2.color_description FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code WHERE t1.product_name  =  \"chervil\"
SELECT t1.country FROM inst AS t1 JOIN authorship AS t2 ON t1.instid  =  t2.instid JOIN papers AS t3 ON t2.paperid  =  t3.paperid GROUP BY t1.country ORDER BY count(*) DESC LIMIT 1
SELECT t1.customer_name ,  t1.customer_phone FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t3.state_province_county  =  'California'
SELECT t1.customer_name ,  t2.active_from_date FROM customers AS t1 JOIN customer_contact_channels AS t2 ON t1.customer_id  =  t2.customer_id WHERE t2.channel_code  =  'Email'
SELECT t1.customer_name ,  t2.order_date FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id WHERE order_status  =  \"Delivered\"
SELECT t1.customer_name FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t3.state_province_county  =  \"Colorado\"
SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id GROUP BY t1.customer_name ORDER BY sum(t3.order_quantity) DESC LIMIT 1
SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id JOIN products AS t4 ON t3.product_id  =  t4.product_id WHERE t4.product_details  =  'Latte' INTERSECT SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id JOIN products AS t4 ON t3.product_id  =  t4.product_id WHERE t4.product_details  =  'Americano'
SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id WHERE t3.order_quantity  =  ( SELECT max(order_quantity) FROM order_items)
SELECT t1.customer_name FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id ORDER BY t2.order_date DESC LIMIT 1
SELECT t1.customer_name FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id GROUP BY t1.customer_name ORDER BY count(*) DESC LIMIT 1
SELECT t1.customer_name FROM customers AS t1 JOIN first_notification_of_loss AS t2 ON t1.customer_id  =  t2.customer_id JOIN services AS t3 ON t2.service_id  =  t3.service_id WHERE t3.service_name  =  \"Close a policy\" INTERSECT SELECT t1.customer_name FROM customers AS t1 JOIN first_notification_of_loss AS t2 ON t1.customer_id  =  t2.customer_id JOIN services AS t3 ON t2.service_id  =  t3.service_id WHERE t3.service_name  =  \"New policy application\"
SELECT t1.customer_name FROM customers AS t1 JOIN first_notification_of_loss AS t2 ON t1.customer_id  =  t2.customer_id JOIN services AS t3 ON t2.service_id  =  t3.service_id WHERE t3.service_name  =  \"Close a policy\" OR t3.service_name  =  \"Upgrade a policy\"
SELECT t1.dname FROM department AS t1 JOIN dept_locations AS t2 ON t1.dnumber  =  t2.dnumber WHERE t2.dlocation  =  'Houston'
SELECT t1.document_name FROM documents AS t1 JOIN document_sections AS t2 ON t1.document_code  =  t2.document_code GROUP BY t1.document_code ORDER BY count(*) DESC LIMIT 1
SELECT t1.email_address FROM customers AS t1 JOIN complaints AS t2 ON t1.customer_id  =  t2.customer_id GROUP BY t1.customer_id ORDER BY count(*) LIMIT 1
SELECT t1.first_name FROM staff AS t1 JOIN complaints AS t2 ON t1.staff_id  =  t2.staff_id GROUP BY t2.staff_id ORDER BY count(*) LIMIT 5
SELECT t1.fname ,  t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t3.title  =  \"Nameless ,  Painless\"
SELECT t1.fname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t3.title LIKE \"%Functional%\"
SELECT t1.form_name FROM forms AS t1 JOIN party_forms AS t2 ON t1.form_id  =  t2.form_id GROUP BY t2.form_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.gdp ,  t1.Regional_Population FROM city AS T1 JOIN hosting_city AS T2 ON T1.city_id  =  T2.host_city GROUP BY t2.Host_City HAVING count(*)  >  1
SELECT t1.id ,  t1.name ,  t1.age FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id GROUP BY t1.id HAVING count(*)  >  1
SELECT t1.last_name FROM staff AS t1 JOIN complaints AS t2 ON t1.staff_id  =  t2.staff_id JOIN products AS t3 ON t2.product_id  =  t3.product_id ORDER BY t3.product_price LIMIT 1
SELECT t1.last_name FROM staff AS t1 JOIN complaints AS t2 ON t1.staff_id  =  t2.staff_id ORDER BY t2.date_complaint_raised LIMIT 1
SELECT t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid GROUP BY t1.fname ,  t1.lname ORDER BY count(*) DESC LIMIT 1
SELECT t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t3.title  =  \"Binders Unbound\"
SELECT t1.lname FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t3.title LIKE \"%Monadic%\"
SELECT t1.manufacturer_id ,  t1.num_of_shops FROM manufacturer AS t1 JOIN furniture_manufacte AS t2 ON t1.manufacturer_id  =  t2.manufacturer_id ORDER BY t2.Price_in_Dollar DESC LIMIT 1
SELECT t1.name ,  t1.age FROM pilot AS t1 JOIN MATCH AS t2 ON t1.pilot_id  =  t2.winning_pilot ORDER BY t1.age LIMIT 1
SELECT t1.name ,  t1.age FROM pilot AS t1 JOIN MATCH AS t2 ON t1.pilot_id  =  t2.winning_pilot WHERE t1.age  <  30 GROUP BY t2.winning_pilot ORDER BY count(*) DESC LIMIT 1
SELECT t1.name ,  t1.age FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id ORDER BY t2.num_of_ticket DESC LIMIT 1
SELECT t1.name ,  t1.capacity FROM stadium AS t1 JOIN event AS t2 ON t1.id  =  t2.stadium_id WHERE t2.name  =  'World Junior'
SELECT t1.name ,  t1.nationality FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id WHERE RESULT  =  'Win' GROUP BY t2.swimmer_id HAVING count(*)  >  1
SELECT t1.name ,  t1.sex ,  min(oppose_rate) FROM people AS t1 JOIN candidate AS t2 ON t1.people_id  =  t2.people_id GROUP BY t1.sex
SELECT t1.name ,  t1.sex FROM people AS t1 JOIN candidate AS t2 ON t1.people_id  =  t2.people_id ORDER BY t2.support_rate DESC LIMIT 1
SELECT t1.name ,  t2.price_in_dollar FROM furniture AS t1 JOIN furniture_manufacte AS t2 ON t1.Furniture_ID  =  t2.Furniture_ID
SELECT t1.name FROM bridge AS t1 JOIN architect AS t2 ON t1.architect_id  =  t2.id WHERE t2.nationality  =  'American' ORDER BY t1.length_feet
SELECT t1.name FROM channel AS t1 JOIN broadcast AS t2 ON t1.channel_id  =  t2.channel_id WHERE t2.time_of_day  =  'Morning'
SELECT t1.name FROM channel AS t1 JOIN broadcast AS t2 ON t1.channel_id  =  t2.channel_id WHERE t2.time_of_day  =  'Morning' INTERSECT SELECT t1.name FROM channel AS t1 JOIN broadcast AS t2 ON t1.channel_id  =  t2.channel_id WHERE t2.time_of_day  =  'Night'
SELECT t1.name FROM driver AS t1 JOIN school_bus AS t2 ON t1.driver_id  =  t2.driver_id ORDER BY years_working DESC LIMIT 1
SELECT t1.name FROM employee AS t1 JOIN evaluation AS t2 ON t1.Employee_ID  =  t2.Employee_ID GROUP BY t2.Employee_ID ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM employee AS t1 JOIN evaluation AS t2 ON t1.Employee_ID  =  t2.Employee_ID ORDER BY t2.bonus DESC LIMIT 1
SELECT t1.name FROM furniture AS t1 JOIN furniture_manufacte AS t2 ON t1.Furniture_ID  =  t2.Furniture_ID WHERE t2.Price_in_Dollar  <  (SELECT max(Price_in_Dollar) FROM furniture_manufacte)
SELECT t1.name FROM inst AS t1 JOIN authorship AS t2 ON t1.instid  =  t2.instid JOIN papers AS t3 ON t2.paperid  =  t3.paperid GROUP BY t1.name ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM people AS t1 JOIN candidate AS t2 ON t1.people_id  =  t2.people_id WHERE t1.sex  =  'F' ORDER BY t1.name
SELECT t1.name FROM people AS t1 JOIN candidate AS t2 ON t1.people_id  =  t2.people_id WHERE t2.support_rate  <  t2.oppose_rate
SELECT t1.name FROM program AS t1 JOIN broadcast AS t2 ON t1.program_id  =  t2.program_id GROUP BY t2.program_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM ship AS t1 JOIN captain AS t2 ON t1.ship_id  =  t2.ship_id GROUP BY t2.ship_id HAVING count(*)  >  1
SELECT t1.name FROM ship AS t1 JOIN captain AS t2 ON t1.ship_id  =  t2.ship_id ORDER BY t2.age LIMIT 1
SELECT t1.name FROM ship AS t1 JOIN captain AS t2 ON t1.ship_id  =  t2.ship_id WHERE t2.rank  =  'Midshipman' INTERSECT SELECT t1.name FROM ship AS t1 JOIN captain AS t2 ON t1.ship_id  =  t2.ship_id WHERE t2.rank  =  'Lieutenant'
SELECT t1.name FROM stadium AS t1 JOIN event AS t2 ON t1.id  =  t2.stadium_id GROUP BY t2.stadium_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id GROUP BY t2.swimmer_id HAVING count(*)  >=  2
SELECT t1.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id GROUP BY t2.swimmer_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id WHERE RESULT  =  'Win'
SELECT t1.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id WHERE RESULT  =  'Win' INTERSECT SELECT t1.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id WHERE RESULT  =  'Loss'
SELECT t1.name FROM train AS t1 JOIN route AS t2 ON t1.id  =  t2.train_id GROUP BY t2.train_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.name FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id JOIN museum AS t3 ON t3.Museum_ID  =  t2.Museum_ID WHERE t3.open_year  <  2009 INTERSECT SELECT t1.name FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id JOIN museum AS t3 ON t3.Museum_ID  =  t2.Museum_ID WHERE t3.open_year  >  2011
SELECT t1.organization_name FROM organizations AS t1 JOIN organization_contact_individuals AS t2 ON t1.organization_id  =  t2.organization_id GROUP BY t1.organization_name ORDER BY count(*) DESC LIMIT 1
SELECT t1.owner FROM program AS t1 JOIN broadcast AS t2 ON t1.program_id  =  t2.program_id WHERE t2.Time_of_day  =  \"Morning\" INTERSECT SELECT t1.owner FROM program AS t1 JOIN broadcast AS t2 ON t1.program_id  =  t2.program_id WHERE t2.Time_of_day  =  \"Night\"
SELECT t1.party_email FROM parties AS t1 JOIN party_forms AS t2 ON t1.party_id  =  t2.party_id WHERE t2.form_id  =  (SELECT form_id FROM party_forms GROUP BY form_id ORDER BY count(*) DESC LIMIT 1)
SELECT t1.party_email FROM parties AS t1 JOIN party_services AS t2 ON t1.party_id  =  t2.customer_id GROUP BY t1.party_email ORDER BY count(*) DESC LIMIT 1
SELECT t1.payment_method FROM customers AS t1 JOIN customer_orders AS t2 ON t1.customer_id  =  t2.customer_id JOIN order_items AS t3 ON t2.order_id  =  t3.order_id GROUP BY t1.customer_name ORDER BY sum(t3.order_quantity) LIMIT 1
SELECT t1.phone_number FROM customers AS t1 JOIN complaints AS t2 ON t1.customer_id  =  t2.customer_id ORDER BY t2.date_complaint_raised DESC LIMIT 1
SELECT t1.pixels FROM screen_mode AS t1 JOIN phone AS t2 ON t1.Graphics_mode  =  t2.screen_mode WHERE t2.Accreditation_type  =  'Provisional' INTERSECT SELECT t1.pixels FROM screen_mode AS t1 JOIN phone AS t2 ON t1.Graphics_mode  =  t2.screen_mode WHERE t2.Accreditation_type  =  'Full'
SELECT t1.product FROM product AS t1 JOIN store_product AS t2 ON t1.product_id  =  t2.product_id JOIN store AS t3 ON t2.store_id  =  t3.store_id WHERE t3.store_name  =  \"Miramichi\"
SELECT t1.product_id ,  t2.color_description FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code JOIN product_characteristics AS t3 ON t1.product_id  =  t3.product_id GROUP BY t1.product_id HAVING count(*)  >=  2
SELECT t1.product_name ,   t1.product_price FROM products AS t1 JOIN regular_order_products AS t2 ON t1.product_id  =  t2.product_id GROUP BY t2.product_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.product_name ,  count(*) FROM products AS t1 JOIN complaints AS t2 ON t1.product_id  =  t2.product_id GROUP BY t1.product_name
SELECT t1.product_name ,  t1.typical_buying_price ,  t1.typical_selling_price FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code WHERE t2.color_description  =  \"yellow\"
SELECT t1.product_name FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code WHERE t2.color_description  =  \"white\"
SELECT t1.product_name FROM products AS t1 JOIN ref_product_categories AS t2 ON t1.product_category_code  =  t2.product_category_code JOIN ref_colors AS t3 ON t1.color_code  =  t3.color_code WHERE t3.color_description  =  \"white\" AND t2.unit_of_measure != \"Handful\"
SELECT t1.route_name FROM Delivery_Routes AS t1 JOIN Delivery_Route_Locations AS t2 ON t1.route_id  =  t2.route_id GROUP BY t1.route_id ORDER BY count(*) DESC LIMIT 1
SELECT t1.sex FROM people AS t1 JOIN candidate AS t2 ON t1.people_id  =  t2.people_id GROUP BY t1.sex ORDER BY avg(t2.unsure_rate) DESC LIMIT 1
SELECT t1.store_name FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id WHERE t3.district_name  =  \"Khanewal District\"
SELECT t1.store_name FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id WHERE district_id  =  (SELECT district_id FROM district ORDER BY city_population DESC LIMIT 1)
SELECT t1.title FROM film AS T1 JOIN film_market_estimation AS T2  ON T1.Film_ID  =  T2.Film_ID ORDER BY high_estimate DESC LIMIT 1
SELECT t2.All_Games ,  t1.location FROM university AS t1 JOIN basketball_match AS t2 ON t1.school_id  =  t2.school_id WHERE team_name  =  'Clemson'
SELECT t2.Museum_ID ,  t1.name FROM museum AS t1 JOIN visit AS t2 ON t1.Museum_ID  =  t2.Museum_ID GROUP BY t2.Museum_ID ORDER BY count(*) DESC LIMIT 1
SELECT t2.catalog_level_name FROM catalog_contents AS t1 JOIN catalog_structure AS t2 ON t1.catalog_level_number  =  t2.catalog_level_number ORDER BY t1.price_in_dollars LIMIT 1
SELECT t2.claim_status_name FROM claims_processing AS t1 JOIN claims_processing_stages AS t2 ON t1.claim_stage_id  =  t2.claim_stage_id GROUP BY t1.claim_stage_id ORDER BY count(*) DESC LIMIT 1
SELECT t2.color_description FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code GROUP BY t2.color_description ORDER BY count(*) ASC LIMIT 1
SELECT t2.color_description FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code GROUP BY t2.color_description ORDER BY count(*) DESC LIMIT 1
SELECT t2.color_description FROM products AS t1 JOIN ref_colors AS t2 ON t1.color_code  =  t2.color_code WHERE t1.product_name  =  \"catnip\"
SELECT t2.customer_details FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id GROUP BY t2.customer_details ORDER BY count(*) DESC LIMIT 1
SELECT t2.customer_details FROM policies AS t1 JOIN customers AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.start_date  =  (SELECT max(start_date) FROM policies)
SELECT t2.date_opened ,  t2.date_closed FROM customers AS t1 JOIN customers_policies AS t2 ON t1.customer_id  =  t2.customer_id WHERE t1.customer_name LIKE \"%Diana%\"
SELECT t2.document_structure_description FROM documents AS t1 JOIN document_structures AS t2 ON t1.document_structure_code  =  t2.document_structure_code GROUP BY t1.document_structure_code ORDER BY count(*) DESC LIMIT 1
SELECT t2.firstname FROM Performance AS t1 JOIN Band AS t2 ON t1.bandmate  =  t2.id JOIN Songs AS T3 ON T3.SongId  =  T1.SongId GROUP BY firstname ORDER BY count(*) DESC LIMIT 1
SELECT t2.id ,  t2.local_authority FROM weekly_weather AS t1 JOIN station AS t2 ON t1.station_id  =  t2.id GROUP BY t1.station_id HAVING max(t1.precipitation)  >  50
SELECT t2.id ,  t2.local_authority FROM weekly_weather AS t1 JOIN station AS t2 ON t1.station_id  =  t2.id GROUP BY t1.station_id ORDER BY avg(high_temperature) DESC LIMIT 1
SELECT t2.name FROM hiring AS t1 JOIN shop AS t2 ON t1.shop_id  =  t2.shop_id GROUP BY t1.shop_id ORDER BY count(*) DESC LIMIT 1
SELECT t2.product_details ,  t2.product_id FROM order_items AS t1 JOIN products AS t2 ON t1.product_id  =  t2.product_id GROUP BY t1.product_id ORDER BY sum(t1.order_quantity) LIMIT 1
SELECT t2.product_details FROM order_items AS t1 JOIN products AS t2 ON t1.product_id  =  t2.product_id GROUP BY t1.product_id ORDER BY count(*) DESC LIMIT 1
SELECT t2.section_title FROM documents AS t1 JOIN document_sections AS t2 ON t1.document_code  =  t2.document_code WHERE t1.document_name  =  \"David CV\"
SELECT t2.service_name FROM first_notification_of_loss AS t1 JOIN services AS t2 ON t1.service_id  =  t2.service_id GROUP BY t1.service_id HAVING count(*)  >  2
SELECT t2.state_province_county ,  count(*) FROM customer_addresses AS t1 JOIN addresses AS t2 ON t1.address_id  =  t2.address_id GROUP BY t2.state_province_county
SELECT t2.team_name ,  t2.ACC_Regular_Season FROM university AS t1 JOIN basketball_match AS t2 ON t1.school_id  =  t2.school_id ORDER BY t1.founded LIMIT 1
SELECT t2.team_name FROM university AS t1 JOIN basketball_match AS t2 ON t1.school_id  =  t2.school_id WHERE enrollment  <  (SELECT avg(enrollment) FROM university)
SELECT t2.title FROM authorship AS t1 JOIN papers AS t2 ON t1.paperid  =  t2.paperid WHERE t1.authorder  =  (SELECT max(authorder) FROM authorship)
SELECT t2.unit_of_measure ,  t2.product_category_code FROM products AS t1 JOIN ref_product_categories AS t2 ON t1.product_category_code  =  t2.product_category_code WHERE t1.product_name  =  \"chervil\"
SELECT t2.unit_of_measure FROM products AS t1 JOIN ref_product_categories AS t2 ON t1.product_category_code  =  t2.product_category_code WHERE t1.product_name  =  \"cumin\"
SELECT t2.visitor_id ,  t1.name ,  t1.Level_of_membership FROM visitor AS t1 JOIN visit AS t2 ON t1.id  =  t2.visitor_id GROUP BY t2.visitor_id ORDER BY sum(t2.Total_spent) DESC LIMIT 1
SELECT t3.District_name FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id WHERE t1.Type  =  \"City Mall\" INTERSECT SELECT t3.District_name FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id WHERE t1.Type  =  \"Village Store\"
SELECT t3.address_content FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t1.customer_name  =  \"Maudie Kertzmann\"
SELECT t3.characteristic_name ,  t3.characteristic_data_type FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"cumin\"
SELECT t3.characteristic_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id GROUP BY t3.characteristic_name HAVING count(*)  >= 2
SELECT t3.characteristic_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id GROUP BY t3.characteristic_name ORDER BY count(*) DESC LIMIT 1
SELECT t3.characteristic_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"sesame\"
SELECT t3.characteristic_name FROM products AS t1 JOIN product_characteristics AS t2 ON t1.product_id  =  t2.product_id JOIN CHARACTERISTICS AS t3 ON t2.characteristic_id  =  t3.characteristic_id WHERE t1.product_name  =  \"sesame\" AND t3.characteristic_type_code  =  \"Grade\"
SELECT t3.city FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id GROUP BY t3.city ORDER BY count(*) DESC LIMIT 1
SELECT t3.customer_details FROM claim_headers AS t1 JOIN policies AS t2 ON t1.policy_id  =  t2.policy_id JOIN customers AS t3 ON t2.customer_id  =  t3.customer_id WHERE t1.amount_claimed  =  (SELECT max(amount_claimed) FROM claim_headers)
SELECT t3.customer_details FROM claim_headers AS t1 JOIN policies AS t2 ON t1.policy_id  =  t2.policy_id JOIN customers AS t3 ON t2.customer_id  =  t3.customer_id WHERE t1.amount_piad  =  (SELECT min(amount_piad) FROM claim_headers)
SELECT t3.fname ,  t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\" AND t2.position  =  \"President\"
SELECT t3.fname ,  t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\" AND t3.major  =  \"600\"
SELECT t3.fname ,  t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\" AND t3.sex  =  \"F\"
SELECT t3.fname ,  t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Hopkins Student Enterprises\" AND t2.position  =  \"CTO\"
SELECT t3.fname ,  t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Hopkins Student Enterprises\" AND t3.sex  =  \"M\"
SELECT t3.headquartered_city ,  count(*) FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id GROUP BY t3.headquartered_city
SELECT t3.headquartered_city FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id GROUP BY t3.headquartered_city ORDER BY count(*) DESC LIMIT 1
SELECT t3.headquartered_city FROM store AS t1 JOIN store_district AS t2 ON t1.store_id  =  t2.store_id JOIN district AS t3 ON t2.district_id  =  t3.district_id WHERE t1.store_name  =  \"Blackville\"
SELECT t3.individual_last_name FROM organizations AS t1 JOIN organization_contact_individuals AS t2 ON t1.organization_id  =  t2.organization_id JOIN individuals AS t3 ON t2.individual_id  =  t3.individual_id WHERE t1.organization_name  =  \"Labour Party\" ORDER BY t2.date_contact_to DESC LIMIT 1
SELECT t3.individual_last_name FROM organizations AS t1 JOIN organization_contact_individuals AS t2 ON t1.organization_id  =  t2.organization_id JOIN individuals AS t3 ON t2.individual_id  =  t3.individual_id WHERE t1.uk_vat_number  =  (SELECT max(uk_vat_number) FROM organizations) ORDER BY t2.date_contact_to ASC LIMIT 1
SELECT t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Bootup Baltimore\"
SELECT t3.lname FROM club AS t1 JOIN member_of_club AS t2 ON t1.clubid  =  t2.clubid JOIN student AS t3 ON t2.stuid  =  t3.stuid WHERE t1.clubname  =  \"Hopkins Student Enterprises\"
SELECT t3.name ,  t3.time FROM station AS t1 JOIN route AS t2 ON t1.id  =  t2.station_id JOIN train AS t3 ON t2.train_id  =  t3.id WHERE t1.local_authority  =  \"Chiltern\"
SELECT t3.name FROM furniture AS t1 JOIN furniture_manufacte AS t2 ON t1.Furniture_ID  =  t2.Furniture_ID JOIN manufacturer AS t3 ON t2.manufacturer_id  =  t3.manufacturer_id WHERE t1.num_of_component  <  6 INTERSECT SELECT t3.name FROM furniture AS t1 JOIN furniture_manufacte AS t2 ON t1.Furniture_ID  =  t2.Furniture_ID JOIN manufacturer AS t3 ON t2.manufacturer_id  =  t3.manufacturer_id WHERE t1.num_of_component  >  10
SELECT t3.name FROM record AS t1 JOIN event AS t2 ON t1.event_id  =  t2.id JOIN stadium AS t3 ON t3.id  =  t2.stadium_id GROUP BY t2.stadium_id ORDER BY count(*) DESC LIMIT 1
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid JOIN inst AS t4 ON t2.instid  =  t4.instid WHERE t4.country  =  \"Japan\" AND t2.authorder  =  1 AND t1.lname  =  \"Ohori\"
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid JOIN inst AS t4 ON t2.instid  =  t4.instid WHERE t4.country  =  \"USA\" AND t2.authorder  =  2 AND t1.lname  =  \"Turon\"
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE  t1.fname  =  \"Aaron\" AND t1.lname  =  \"Turon\"
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE  t1.fname  =  \"Jeremy\" AND t1.lname  =  \"Gibbons\"
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t1.fname  =  \"Olin\" AND t1.lname  =  \"Shivers\"
SELECT t3.title FROM authors AS t1 JOIN authorship AS t2 ON t1.authid  =  t2.authid JOIN papers AS t3 ON t2.paperid  =  t3.paperid WHERE t1.fname  =  \"Stephanie\" AND t1.lname  =  \"Weirich\"
SELECT t4.name FROM swimmer AS t1 JOIN record AS t2 ON t1.id  =  t2.swimmer_id JOIN event AS t3 ON t2.event_id  =  t3.id JOIN stadium AS t4 ON t4.id  =  t3.stadium_id WHERE t1.nationality  =  'Australia'
SELECT team_name FROM basketball_match ORDER BY All_Home DESC
SELECT template_id ,  count(*) FROM Documents GROUP BY template_id
SELECT template_id ,  version_number ,  template_type_code FROM Templates
SELECT template_id FROM Documents GROUP BY template_id HAVING count(*)  >  1
SELECT template_id FROM Templates EXCEPT SELECT template_id FROM Documents
SELECT template_id FROM Templates WHERE template_type_code  =  \"PP\" OR template_type_code  =  \"PPT\"
SELECT template_type_code ,  count(*) FROM Templates GROUP BY template_type_code
SELECT template_type_code ,  template_type_description FROM Ref_template_types
SELECT template_type_code FROM Ref_template_types WHERE template_type_description  =  \"Book\"
SELECT template_type_code FROM Templates EXCEPT SELECT template_type_code FROM Templates AS T1 JOIN Documents AS T2 ON T1.template_id  =  T2.template_id
SELECT template_type_code FROM Templates GROUP BY template_type_code HAVING count(*)  <  3
SELECT template_type_code FROM Templates GROUP BY template_type_code ORDER BY count(*) DESC LIMIT 1
SELECT template_type_description FROM Ref_template_types WHERE template_type_code  =  \"AD\"
SELECT test_result ,  COUNT(*) FROM Student_Tests_Taken GROUP BY test_result ORDER BY COUNT(*) DESC
SELECT text FROM tweets ORDER BY createdate
SELECT text FROM tweets WHERE text LIKE '%intern%'
SELECT theme ,  YEAR FROM exhibition ORDER BY ticket_price DESC
SELECT theme ,  YEAR FROM exhibition WHERE ticket_price  <  15
SELECT title ,  Directed_by FROM Cartoon ORDER BY Original_air_date
SELECT title ,  Studio FROM film WHERE Studio LIKE \"%Universal%\"
SELECT title ,  YEAR ,  director FROM movie ORDER BY budget_million
SELECT title ,  directed_by FROM film
SELECT title ,  directed_by FROM film WHERE film_id NOT IN (SELECT film_id FROM schedule)
SELECT title ,  director FROM film WHERE film_id NOT IN (SELECT film_id FROM film_market_estimation AS T1 JOIN market AS T2 ON T1.market_id  =  T2.Market_ID WHERE country  =  'China')
SELECT title ,  director FROM movie WHERE YEAR  <=  2000 ORDER BY gross_worldwide DESC LIMIT 1
SELECT title ,  film_id FROM film WHERE rental_rate  =  0.99 INTERSECT SELECT T1.title ,  T1.film_id FROM film AS T1 JOIN inventory AS T2 ON T1.film_id  =  T2.film_id GROUP BY T1.film_id HAVING count(*)  <  3
SELECT title ,  rental_rate FROM film ORDER BY rental_rate DESC LIMIT 1
SELECT title FROM Movie WHERE YEAR  =  (SELECT max(YEAR) FROM Movie)
SELECT title FROM Movie WHERE YEAR  >  (SELECT max(YEAR) FROM Movie WHERE director  =  \"Steven Spielberg\")
SELECT title FROM Movie WHERE director  =  \"James Cameron\" OR YEAR   <  1980
SELECT title FROM Movie WHERE director = 'James Cameron' AND YEAR  >  2000
SELECT title FROM Movie WHERE director = 'Steven Spielberg'
SELECT title FROM Movie WHERE mID NOT IN (SELECT mID FROM Rating)
SELECT title FROM course GROUP BY title HAVING count(*)  >  1
SELECT title FROM course ORDER BY title ,  credits
SELECT title FROM course WHERE course_id IN (SELECT T1.course_id FROM prereq AS T1 JOIN course AS T2 ON T1.prereq_id  =  T2.course_id WHERE T2.title  =  'Differential Geometry')
SELECT title FROM course WHERE course_id IN (SELECT T1.prereq_id FROM prereq AS T1 JOIN course AS T2 ON T1.course_id  =  T2.course_id WHERE T2.title  =  'International Finance')
SELECT title FROM course WHERE course_id IN (SELECT T1.prereq_id FROM prereq AS T1 JOIN course AS T2 ON T1.course_id  =  T2.course_id WHERE T2.title  =  'Mobile Computing')
SELECT title FROM course WHERE course_id NOT IN (SELECT course_id FROM prereq)
SELECT title FROM course WHERE dept_name  =  'Statistics' EXCEPT SELECT title FROM course WHERE dept_name  =  'Psychology'
SELECT title FROM course WHERE dept_name  =  'Statistics' INTERSECT SELECT title FROM course WHERE dept_name  =  'Psychology'
SELECT title FROM film WHERE LENGTH  >  100 OR rating  =  'PG' EXCEPT SELECT title FROM film WHERE replacement_cost  >  200
SELECT title FROM film WHERE rating  =  'R'
SELECT title FROM film WHERE special_features LIKE '%Deleted Scenes%'
SELECT title FROM item ORDER BY title
SELECT title FROM item WHERE i_id NOT IN (SELECT i_id FROM review)
SELECT title FROM papers WHERE title LIKE \"%Database%\"
SELECT title FROM papers WHERE title LIKE \"%ML%\"
SELECT title FROM songs WHERE title LIKE '% the %'
SELECT title FROM vocals AS T1 JOIN songs AS T2 ON T1.songid  =  T2.songid GROUP BY T1.songid ORDER BY count(*) DESC LIMIT 1
SELECT tourney_name FROM matches GROUP BY tourney_name HAVING count(*)  >  10
SELECT town_city FROM addresses UNION SELECT state_province_county FROM addresses
SELECT town_city FROM customers WHERE customer_type_code  =  \"Good Credit Rating\" GROUP BY town_city ORDER BY count(*) LIMIT 1
SELECT trade_name ,  count(*) FROM medicine GROUP BY trade_name
SELECT train_number ,  TIME FROM train WHERE origin  =  'Chennai' AND destination  =  'Guruvayur'
SELECT train_number ,  name FROM train ORDER BY TIME
SELECT transaction_id FROM Financial_transactions WHERE transaction_amount  >  (SELECT avg(transaction_amount) FROM Financial_transactions)
SELECT transaction_id FROM TRANSACTIONS WHERE transaction_type_code  =  'PUR'
SELECT transaction_type ,  count(*) FROM Financial_transactions GROUP BY transaction_type
SELECT transaction_type ,  sum(transaction_amount) FROM Financial_transactions GROUP BY transaction_type
SELECT transaction_type FROM Financial_transactions GROUP BY transaction_type ORDER BY sum(transaction_amount) DESC LIMIT 1
SELECT transaction_type_code ,  avg(amount_of_transaction) FROM TRANSACTIONS GROUP BY transaction_type_code
SELECT transaction_type_code ,  max(share_count) ,  min(share_count) FROM TRANSACTIONS GROUP BY transaction_type_code
SELECT transaction_type_code FROM TRANSACTIONS GROUP BY transaction_type_code ORDER BY COUNT(*) ASC LIMIT 1
SELECT transaction_type_code FROM TRANSACTIONS GROUP BY transaction_type_code ORDER BY COUNT(*) DESC LIMIT 1
SELECT transaction_type_description FROM Ref_Transaction_Types WHERE transaction_type_code        =  \"PUR\"
SELECT transcript_date ,  other_details FROM Transcripts ORDER BY transcript_date ASC LIMIT 1
SELECT transcript_date FROM Transcripts ORDER BY transcript_date DESC LIMIT 1
SELECT truck_details FROM trucks ORDER BY truck_licence_number
SELECT unit_of_measure FROM ref_product_categories WHERE product_category_code  =  \"Herbs\"
SELECT user_name ,  password FROM users GROUP BY role_code ORDER BY count(*) DESC LIMIT 1
SELECT vehicle_flight_number ,  date ,  pilot FROM flight ORDER BY altitude ASC
SELECT venue ,  name FROM event ORDER BY Event_Attendance DESC LIMIT 2
SELECT venue FROM MATCH ORDER BY date DESC
SELECT version_number ,  template_type_code FROM Templates WHERE version_number  >  5
SELECT vote_id ,  phone_number ,  state FROM votes
SELECT weight FROM pets ORDER BY pet_age LIMIT 1
SELECT winner_name ,  loser_name FROM matches ORDER BY minutes DESC LIMIT 1
SELECT winner_name ,  winner_rank_points FROM matches GROUP BY winner_name ORDER BY count(*) DESC LIMIT 1
SELECT winner_name FROM matches WHERE YEAR  =  2013 INTERSECT SELECT winner_name FROM matches WHERE YEAR  =  2016
SELECT winner_name FROM matches WHERE tourney_name  =  'Australian Open' ORDER BY winner_rank_points DESC LIMIT 1
SELECT year_opened FROM track WHERE seating BETWEEN 4000 AND 5000
SELECT zip_code  ,  avg(mean_temperature_f) FROM weather WHERE date LIKE \"8/%\" GROUP BY zip_code
SELECT zip_code , count(*) FROM weather WHERE max_wind_Speed_mph  >=  25 GROUP BY zip_code
SELECT zip_code FROM weather GROUP BY zip_code HAVING avg(mean_humidity)  <  70 INTERSECT SELECT zip_code FROM trip GROUP BY zip_code HAVING count(*)  >=  100
SELECT zip_code FROM weather GROUP BY zip_code HAVING avg(mean_visibility_miles)  <  10
SELECT zip_code FROM weather GROUP BY zip_code ORDER BY avg(mean_sea_level_pressure_inches) LIMIT 1
SELECT zip_postcode FROM Addresses WHERE city  =  'Port Chelsea'
select avg(age) from student where stuid not in (select stuid from has_pet)
select avg(capacity) ,  max(capacity) from stadium
select cell_mobile_number from students where first_name  =  'timmothy' and last_name  =  'ward'
select citizenship from singer group by citizenship order by count(*) desc limit 1
select contestant_name from contestants where contestant_name like \"%al%\"
select count(*) ,  t1.stuid from student as t1 join has_pet as t2 on t1.stuid  =  t2.stuid group by t1.stuid
select count(*) from concert where stadium_id = (select stadium_id from stadium order by capacity desc limit 1)
select count(*) from countries as t1 join car_makers as t2 on t1.countryid  =  t2.country group by t1.countryid having count(*)  >  2
select count(*) from dogs where dog_id not in ( select dog_id from treatments )
select distinct t3.name from country as t1 join countrylanguage as t2 on t1.code  =  t2.countrycode join city as t3 on t1.code  =  t3.countrycode where t2.isofficial  =  't' and t2.language  =  'chinese' and t1.continent  =  \"asia\"
select max(capacity), average from stadium
select max(mpg) from cars_data where cylinders  =  8 or year  <  1980
select min(weight) from cars_data where cylinders  =  8 and year  =  1974
select name from city where population between 160000 and 900000
select name from dogs where dog_id not in ( select dog_id from treatments group by dog_id having sum(cost_of_treatment)  >  1000 )
select name from teacher where hometown != \"little lever urban district\"
select other_details from paragraphs where paragraph_text like 'korea'
select production_code ,  channel from cartoon order by original_air_date desc limit 1
select sum(population) ,  avg(surfacearea) from country where continent  =  \"north america\" and surfacearea  >  3000
select t1.first_name from students as t1 join addresses as t2 on t1.permanent_address_id  =  t2.address_id where t2.country  =  'haiti' or t1.cell_mobile_number  =  '09700166582'
select t1.fname from student as t1 join has_pet as t2 on t1.stuid  =  t2.stuid join pets as t3 on t3.petid  =  t2.petid where t3.pettype  =  'cat' intersect select t1.fname from student as t1 join has_pet as t2 on t1.stuid  =  t2.stuid join pets as t3 on t3.petid  =  t2.petid where t3.pettype  =  'dog'
select t1.name from country as t1 join countrylanguage as t2 on t1.code  =  t2.countrycode where t2.language  =  \"english\" and isofficial  =  \"t\" union select t1.name from country as t1 join countrylanguage as t2 on t1.code  =  t2.countrycode where t2.language  =  \"dutch\" and isofficial  =  \"t\"
select t2.concert_name ,  t2.theme ,  count(*) from singer_in_concert as t1 join concert as t2 on t1.concert_id  =  t2.concert_id group by t2.concert_id
select t2.department_name ,  t1.department_id from degree_programs as t1 join departments as t2 on t1.department_id  =  t2.department_id group by t1.department_id order by count(*) desc limit 1
select t2.name ,  t2.capacity from concert as t1 join stadium as t2 on t1.stadium_id  =  t2.stadium_id where t1.year  >  2013 group by t2.stadium_id order by count(*) desc limit 1
SELECT AVG(word_count) OVER (PARTITION BY EXTRACT(YEAR_QUARTER FROM publish_date)) AS avg_word_count FROM articles WHERE category = 'social_justice' AND location = 'USA' AND YEAR(publish_date) BETWEEN 2021 AND 2022;
SELECT SUM(total_funding) FROM Funding WHERE funding_source = 'California Arts Council' AND region = 'California' AND YEAR(event_date) = 2022
SELECT r.preferred_category, COUNT(*) FROM readers r JOIN readership re ON r.id = re.reader_id WHERE re.gender = 'Male' AND re.country = 'UK' GROUP BY r.preferred_category ORDER BY COUNT(*) DESC LIMIT 5
SELECT country, SUM(posts) as total_posts FROM user GROUP BY country ORDER BY total_posts DESC LIMIT 3;
SELECT 100.0 * COUNT(DISTINCT CASE WHEN outcomes.follow_up_date < patients.start_date + INTERVAL '6 month' AND outcomes.improvement THEN patients.id END) / COUNT(DISTINCT patients.id) AS percentage FROM patients JOIN treatments ON patients.id = treatments.patient_id JOIN outcomes ON treatments.id = outcomes.treatment_id WHERE patients.country = 'Japan' AND treatments.type = 'psychotherapy';
SELECT Platform, COUNT(DISTINCT PlayerID) as PlayersLastWeek FROM PlayerGameHistory WHERE PlayDate >= '2022-02-01' GROUP BY Platform
SELECT MAX(cost) FROM labor_costs WHERE state = 'Oregon'
SELECT MAX(quantity) FROM products WHERE certification IN ('recycled', 'fair_trade') GROUP BY certification HAVING COUNT(DISTINCT certification) = 2
SELECT SUM(assets) FROM (SELECT client_id, assets FROM client_assets WHERE client_id IN (SELECT client_id FROM clients WHERE last_transaction_date >= '2021-10-01' AND last_transaction_date <= '2021-12-31')) clients_assets_q4
SELECT consumption FROM total_consumption WHERE year IN (2015, 2016)
SELECT E.EventCountry, EXTRACT(MONTH FROM E.EventDate) AS Month, COUNT(DISTINCT EP.PlayerID) AS PlayersJoined, COUNT(DISTINCT VR.VRDevice) AS VRDevices, COUNT(DISTINCT G.GameID) AS UniqueGames FROM Players P JOIN EventParticipation EP ON P.PlayerID = EP.PlayerID JOIN EventDates E ON EP.EventID = E.EventID JOIN GamePlay G ON P.PlayerID = G.PlayerID JOIN VRAdoption VR ON P.PlayerID = VR.PlayerID GROUP BY E.EventCountry, EXTRACT(MONTH FROM E.EventDate)
SELECT region, SUM(personnel) FROM military_personnel GROUP BY region ORDER BY SUM(personnel) DESC
SELECT EXTRACT(MONTH FROM trip_start_time) AS month, COUNT(*) AS num_trips
SELECT COUNT(*) FROM Volunteers
SELECT V.Name, SUM(D.Amount) as TotalDonations FROM Volunteers V JOIN VolunteerPrograms VP ON V.VolunteerID = VP.VolunteerID JOIN Donations D ON VP.ProgramID = D.ProgramID GROUP BY V.Name
SELECT City.name, SUM(Property.price) as total_price FROM City INNER JOIN Property ON City.name = Property.city WHERE City.sustainable_rating > 80 AND City.population > 500000 GROUP BY City.name HAVING SUM(Property.price) > (SELECT AVG(Property.price) FROM Property)
SELECT Location, Year, COUNT(*) as NumEvents FROM EsportsEvents GROUP BY Location, Year
SELECT EXTRACT(DOW FROM publication_date) AS day_of_week, COUNT(*) AS count FROM articles WHERE EXTRACT(YEAR FROM publication_date) = 2022 GROUP BY day_of_week;
SELECT E.EventCountry, GD.GameName, GD.Genre, SUM(GD.AvgHoursPlayed) AS TotalHoursPlayed, ROW_NUMBER() OVER (PARTITION BY E.EventCountry ORDER BY SUM(GD.AvgHoursPlayed) DESC) AS Rank FROM Players P JOIN EventParticipation EP ON P.PlayerID = EP.PlayerID JOIN EventDates E ON EP.EventID = E.EventID JOIN GameData GD ON P.PlayerID = GD.GameID GROUP BY E.EventCountry, GD.GameName, GD.Genre HAVING COUNT(DISTINCT P.PlayerID) > 1 ORDER BY E.EventCountry, TotalHoursPlayed DESC
SELECT (COUNT(policies.id) * 100.0 / (SELECT COUNT(*) FROM policies WHERE issue_date BETWEEN '2021-04-01' AND '2021-06-30')) FROM policies JOIN policyholders ON policies.policyholder_id = policyholders.id JOIN zipcodes ON SUBSTRING(policyholders.address FROM 14 FOR 5) = zipcodes.zipcode WHERE zipcodes.area_type = 'Rural' AND policies.issue_date BETWEEN '2021-04-01' AND '2021-06-30';
SELECT farm_id, AVG(soil_moisture) AS avg_moisture FROM farm_soil_moisture WHERE timestamp >= NOW() - INTERVAL '1 year' GROUP BY farm_id ORDER BY avg_moisture DESC LIMIT 3;
SELECT year, AVG(quantity) as avg_quantity FROM production_yearly WHERE element = 'Europium' GROUP BY year
SELECT AVG(risk_level) FROM investments WHERE sector = 'green_technology'
SELECT e.name, e.department FROM Employees e LEFT JOIN Training t ON e.id = t.employee WHERE t.completed IS NULL
SELECT SoilAnalysis.field_id FROM SoilAnalysis WHERE SoilAnalysis.organic_matter > 12 UNION SELECT SatelliteImagery.field_id FROM SatelliteImagery
SELECT COUNT(*) FROM ( (SELECT * FROM retail_union WHERE YEAR(dob) = 2020) UNION (SELECT * FROM transportation_union WHERE YEAR(dob) = 2020) ) AS all_unions;
SELECT JobRole, AVG(Salary) as AvgSalary FROM Employees WHERE HireDate >= (CURRENT_DATE - INTERVAL '12 months') GROUP BY JobRole;
SELECT SUM(Quantity) FROM IndianMines
SELECT SUM(transaction_amount) FROM community_development.transactions WHERE account_number IN (SELECT account_number FROM community_development.accounts WHERE is_community_development = true) AND transaction_date < NOW() - INTERVAL '60 days';
SELECT sql.mineral, sql.country, sql.deposit_size FROM geological_survey sql WHERE sql.mineral = 'Copper' AND sql.country = 'Chile' AND sql.deposit_size = (SELECT MAX(deposit_size) FROM geological_survey WHERE mineral = 'Copper')
SELECT response_type, AVG(EXTRACT(EPOCH FROM response_time)) as avg_response_time FROM emergency_responses GROUP BY response_type;
SELECT CustomerID FROM Investments WHERE Market = 'Canada' AND Value > 5000
SELECT AVG(capacity/population*1000000) FROM Population, LandfillCapacity WHERE Population.country IN ('Germany', 'France') AND LandfillCapacity.country = Population.country
SELECT vulnerability_name, COUNT(*) as occurrences, affected_system FROM vulnerabilities WHERE occurrence_time >= NOW() - INTERVAL '6 months' GROUP BY vulnerability_name, affected_system ORDER BY occurrences DESC LIMIT 5;
SELECT SUM(balance) FROM accounts a JOIN clients c ON a.client_id = c.id WHERE c.id IN (SELECT client_id FROM investments WHERE investment_date >= c.registered_date + INTERVAL '1 week');
SELECT date, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC) as ranking FROM daily_revenue WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date;
SELECT AVG(md.accuracy) FROM model_data md WHERE md.dataset_type = 'balanced'
SELECT chemical_id, environmental_impact_score FROM (SELECT chemical_id, environmental_impact_score, RANK() OVER (ORDER BY environmental_impact_score DESC) as impact_rank FROM environmental_impact) WHERE impact_rank <= 3
SELECT * FROM Donations WHERE donation_date BETWEEN '2022-01-01' AND '2022-01-31'
SELECT f.location, f.name, f.ethical_manufacturing FROM factories f WHERE (f.location, f.ethical_manufacturing) IN (SELECT f1.location, MAX(f1.ethical_manufacturing) FROM factories f1 GROUP BY f1.location);
SELECT DATE_TRUNC('month', d.created_at) as month, SUM(o.price) as revenue FROM dishes d JOIN orders o ON d.dish_id = o.dish_id WHERE d.is_organic = TRUE AND d.created_at >= NOW() - INTERVAL '6 months' GROUP BY month ORDER BY month;
SELECT Country, SUM(Waste) OVER (PARTITION BY Country ORDER BY WasteDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS TotalWaste, RANK() OVER (ORDER BY SUM(Waste) DESC) AS Rank FROM WasteGeneration WHERE WasteDate >= DATEADD(month, -1, CURRENT_TIMESTAMP) GROUP BY Country, WasteDate HAVING Rank <= 3
SELECT country, AVG(temperature - LAG(temperature) OVER (PARTITION BY country ORDER BY year)) AS avg_annual_temp_change FROM temperature_data WHERE country IN (SELECT DISTINCT country FROM temperature_data WHERE country LIKE 'Arctic%') GROUP BY country
SELECT COUNT(*) FROM strandings WHERE date >= (SELECT date FROM (SELECT date FROM strandings ORDER BY date DESC LIMIT 1) subquery) - INTERVAL '3 months';
SELECT COUNT(*) FROM Patents WHERE Company IN ('Waymo', 'Tesla') AND Patent LIKE '%Autonomous%'
INSERT INTO community_policing (id, event_date, event_type) VALUES (3, CURRENT_DATE - INTERVAL '2 weeks', 'Workshop'), (4, CURRENT_DATE, 'Meeting');
INSERT INTO fish_species (species_name, conservation_status) VALUES ('bluefin tuna', 'endangered')
INSERT INTO satellite_image (field_id, image_url, anomaly, timestamp) SELECT f.id, 'https://example.com/image.jpg', false, s.timestamp FROM field f CROSS JOIN generate_series(NOW() - INTERVAL '1 week', NOW(), INTERVAL '1 day') s(timestamp);
INSERT INTO clinics (id, name, service, location, state) VALUES (1, 'Southwest Care', 'Primary Care', 'New Mexico', 'New Mexico'); INSERT INTO clinics (id, name, service, location, state) VALUES (2, 'Desert Sun Pediatrics', 'Pediatrics', 'New Mexico', 'New Mexico'); INSERT INTO clinics (id, name, service, location, state) VALUES (3, 'Mountain Dental', 'Dental', 'New Mexico', 'New Mexico')
INSERT INTO dances (id, name, description, origin_country) VALUES (5, 'Sankirtana', 'Manipuri religious dance drama', 'India'), (6, 'Hula', 'Hawaiian storytelling dance', 'USA')
INSERT INTO crimes (id, district, crime_date, crime_type) VALUES (3, 'District Z', CURRENT_DATE - INTERVAL '3 days', 'Assault'), (4, 'District W', CURRENT_DATE - INTERVAL '1 day', 'Harassment');
INSERT INTO Products (name, rating, cruelty_free) SELECT 'new product', 4.7, 'yes' FROM dual WHERE NOT EXISTS (SELECT * FROM Products WHERE name = 'new product')
INSERT INTO Museums (Name, Attendance, Year) VALUES ('New Museum', 8000, 2021)
INSERT INTO ocean_health (location, temperature, phosphorus) VALUES ('Atlantic Ocean', 20.5, 1.2), ('Pacific Ocean', 25.2, 1.8), ('Indian Ocean', 27.8, 2.3), ('Arctic Ocean', 5.3, 0.5), ('Southern Ocean', 2.8, 0.3)
INSERT INTO sites (id, name, country, year_added) VALUES (4, 'Baalbek', 'Lebanon', 1984)
INSERT INTO manufacturing_output (chemical_name, quantity) SELECT chemical_name, 800 as quantity FROM chemicals WHERE chemical_name = 'He' UNION ALL SELECT 'Kr' as chemical_name, 1500 as quantity WHERE NOT EXISTS (SELECT 1 FROM chemicals WHERE chemical_name = 'Kr');
INSERT INTO cosmetic_ingredients (ingredient_id, product_name, brand_name, ingredient_type) VALUES (NULL, 'Natural Deodorant', 'Ethique', 'Ingredient'); INSERT INTO cosmetic_ingredients (ingredient_id, product_name, brand_name, ingredient_type, ingredient_name) SELECT ingredient_id, 'Natural Deodorant', 'Ethique', 'Ingredient', 'Magnesium Hydroxide' FROM cosmetic_ingredients WHERE ingredient_name = 'Magnesium Hydroxide' UNION ALL SELECT NULL, 'Natural Deodorant', 'Ethique', 'Ingredient', 'Caprylic/Capric Triglyceride' UNION ALL SELECT NULL, 'Natural Deodorant', 'Ethique', 'Ingredient', 'Coconut Oil';
INSERT INTO defense_diplomacy (diplomacy_id, name, country, purpose, start_date) VALUES (1, 'Joint Military Exercise', 'USA', 'Military cooperation', '2023-02-20')
UPDATE skills SET art_type = 'Printmaking' WHERE artist_id = 1
UPDATE clinical_trials SET trial_name = CONCAT(trial_name, ' (', (SELECT drug_name FROM drugs WHERE drugs.drug_id = clinical_trials.drug_id), ')')
UPDATE hospitals SET num_beds = num_beds * 1.25 WHERE location IN ('Urban, California', 'Urban, New York')
UPDATE city_budgets SET budget = budget * 1.1 WHERE city = 'City C' AND sector = 'parks and recreation'
UPDATE fish SET water_temp = AVG(water_temp) OVER (PARTITION BY species ORDER BY date RANGE BETWEEN INTERVAL '1 week' PRECEDING AND CURRENT ROW) WHERE date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week';
UPDATE sites SET disaster_impact = 1 WHERE id = 2
UPDATE soil_conditions SET last_updated = last_updated + INTERVAL '1 day';
UPDATE esports_event SET prize_pool = prize_pool + 20000 WHERE name = 'VirtuaCup'
UPDATE player SET total_games_played = total_games_played + 1 WHERE email = 'player1@email.com'
UPDATE clinical_trials SET trial_name = CONCAT(trial_name, ' (', (SELECT drug_name FROM drugs WHERE drugs.drug_id = clinical_trials.drug_id), ')') WHERE trial_date >= '2022-01-01' AND trial_date < '2023-01-01'
DELETE FROM Artworks WHERE Artist IN (SELECT Artist FROM Artworks GROUP BY Artist HAVING COUNT(Artwork) < 2)
DELETE FROM workforce w WHERE NOT EXISTS (SELECT 1 FROM factories f WHERE f.factory_id = w.factory_id AND f.ethical_manufacturing = true);
DELETE FROM languages WHERE num_speakers < 1000
DELETE FROM player WHERE player_id NOT IN (SELECT player_id FROM player_game WHERE game_id = (SELECT game_id FROM game WHERE name = 'PuzzleParadise'))
DELETE FROM drugs WHERE ema_approval = false
DELETE FROM Products WHERE expiration_date < CURDATE()
DELETE FROM Soil_Moisture WHERE Timestamp < NOW() - INTERVAL '30 days';
DELETE FROM species_observations WHERE observation_date < (CURRENT_DATE - INTERVAL '5 years');
DELETE FROM retailers WHERE last_audit_date < (CURRENT_DATE - INTERVAL '3 years');
DELETE FROM Content WHERE (ContentType, LanguageID) IN (('Blog', 3), ('Blog', 5));
DELETE FROM IoT_Sensors WHERE Timestamp < NOW() - INTERVAL '3 days';
DELETE FROM EmployeeData WHERE (EmployeeID, Salary) IN (SELECT EmployeeID, MAX(Salary) FROM EmployeeData WHERE Department = 'Finance' GROUP BY EmployeeID);
DELETE FROM patient_visits WHERE patient_id IN (SELECT patient_id FROM patient_visits p JOIN (SELECT patient_id, MAX(last_visit_date) AS max_date FROM patient_visits WHERE clinic_id IN (SELECT clinic_id FROM rural_clinics WHERE state = 'Arizona') GROUP BY patient_id) t ON p.patient_id = t.patient_id AND p.last_visit_date < t.max_date - INTERVAL '6 month');
DELETE FROM posts WHERE id IN (SELECT post_id FROM comments c JOIN (SELECT post_id, MIN(created_date) as mindate FROM comments WHERE country = 'Brazil' GROUP BY post_id) d ON c.post_id = d.post_id WHERE c.country = 'Brazil' AND c.created_date < d.mindate)
DELETE FROM VisitorDemographics
DELETE FROM sales WHERE sale_date < (CURRENT_DATE - INTERVAL '6 months');
DELETE FROM Trucks WHERE DATEDIFF(year, PurchaseDate, GETDATE()) > 5
DELETE FROM conditions
DELETE FROM products USING safety_records WHERE products.product_id = safety_records.product_id AND products.is_cruelty_free = FALSE AND safety_records.violation_date >= NOW() - INTERVAL '1 year';
DELETE FROM threat_intelligence WHERE group_name = 'APT29' AND last_updated >= NOW() - INTERVAL '6 months';
DELETE FROM refugee_camps WHERE (id, total_donations) = (SELECT id, MAX(total_donations) FROM refugee_camps);
DELETE FROM subscriber_activity_2 WHERE last_activity_date < (CURRENT_DATE - INTERVAL '90 days') AND region = 'West';
DELETE FROM customers WHERE id NOT IN (SELECT customer_id FROM purchases WHERE purchased_at > NOW() - INTERVAL '1 year');
DELETE FROM Companies WHERE id NOT IN (SELECT company_id FROM Funding)
DELETE FROM SpotifyStreaming WHERE user_id IN (SELECT user_id FROM Users u LEFT JOIN SpotifyStreaming ss ON u.user_id = ss.user_id WHERE ss.timestamp < NOW() - INTERVAL '1 WEEK' GROUP BY user_id HAVING COUNT(ss.stream_id) = 0);
DELETE FROM fairness_metrics WHERE category = 'Explainable AI' AND year = 2021
DELETE FROM donors WHERE last_donation_date < NOW() - INTERVAL '24 months';
DELETE FROM products WHERE last_sale_date < (CURRENT_DATE - INTERVAL '6 months');
DELETE FROM Clients WHERE ID IN (SELECT Clients.ID FROM Clients LEFT JOIN Bills ON Clients.BillID = Bills.ID WHERE DATEDIFF(day, BillingDate, PaymentDate) > 90 AND PaymentDate IS NULL)
