from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver.v2 as uc
#exec_path_chrome = "C:\Program Files\Google\Chrome\Application\chrome.exe" #Do not use this path that is extracted from "chrome:\\version/"
#exec_path_driver = "path/to/chromedriver"

ch_options = Options() #Chrome Options
ch_options.add_argument('--headless')
ch_options.add_argument("--user-data-dir=C:\\Users\\ekans") #Extract this path from "chrome://version/"
ch_options.add_argument('--profile-directory=n3')

user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4844.51 Safari/537.36'
ch_options.add_argument(f'--user-agent={user_agent}')

driver = uc.Chrome(executable_path = ChromeDriverManager().install(), options = ch_options,use_subprocess=True) #Chrome_Options is deprecated. So we use options instead.
#driver = uc.Chrome(version_main=100,options=ch_options)
wait = WebDriverWait(driver, 10)




driver.get(r"https://in.mail.yahoo.com/d/folders/1?guce_referrer=aHR0cHM6Ly9sb2dpbi55YWhvby5jb20v&guce_referrer_sig=AQAAAGCzLPayWHqK4BkEvPjtkxazRhgAC6FVVlOsfjqiC5LM262TK4W87aAK_ngtT2YvdKUPHNW2j0uLD7GP-a3isn4rqKET3qo2_NALDqsot6V2Jvya1iSMyDGIf42ApGApjvYCQMNREv9SxjPLkALa7kyABQBNMWEDgunrOhgX_sQh")
driver.get_screenshot_as_file("yahoo1.png")


'''#After logging in no need to execute further
time.sleep(2)

emailelem = wait.until(EC.presence_of_element_located((By.ID,'login-username')))
emailelem.send_keys('rashminanet@yahoo.com')

wait.until(EC.presence_of_element_located((By.ID,'login-signin')))
emailelem.submit()
time.sleep(2)
driver.get_screenshot_as_file("yahoo2.png")

passwordelem = wait.until(EC.presence_of_element_located((By.ID,'login-passwd')))
passwordelem.send_keys('a1b1c1d1e1')
passwordelem.send_keys(Keys.RETURN)

time.sleep(2)
driver.get_screenshot_as_file("yahoo3.png")
#time.sleep(960)
#time.sleep(9600)
time.sleep(3)
#loginBox = driver.find_element_by_class_name('e_dRA l_T cn_dBP cg_FJ k_w r_P A_6EqO u_e69 p_R S_n C_52qC I_ZamTeg D_F H_6VdP gl_C ab_C en_0 M_1Eu7sD it_dRA is_1SUcgJ cZdTOHS_28Otf4').click()

#compose = driver.find_element_by_css_selector("aria-label='Compose'").click()'''
compose = driver.find_element_by_link_text("Compose").click()
driver.get_screenshot_as_file("yahoo5.png")
time.sleep(3)

send_mail = driver.find_element_by_css_selector("input[class = 'select-input react-typeahead-input input-to Z_N ir_0 j_n y_Z2hYGcu q_52qC k_w W_6D6F H_6NIX M_0 b_0 P_SMJKi A_6EqO D_X p_a L_0 B_0']")
send_mail.send_keys('ekansh.n@gmail.com')
time.sleep(3)
subject = driver.find_element_by_css_selector("input[class='q_T y_Z2hYGcu je_0 jb_0 X_0 N_fq7 G_e A_6EqO C_Z281SGl ir_0 P_0 bj3_Z281SGl b_0 j_n d_72FG em_N']")
subject.send_keys('headless')
time.sleep(3)
body = driver.find_element_by_css_selector("div[class='rte em_N ir_0 iy_A iz_h N_6Fd5']")
body.send_keys('sent by headless chrome')
time.sleep(3)
send = driver.find_element_by_css_selector("button[class='q_Z2aVTcY e_dRA k_w r_P H_6VdP s_3mS2U en_0 M_1gLo4F V_M cZ1RN91d_n y_Z2hYGcu A_6EqO u_e69 b_0 C_52qC I4_Z29WjXl it3_dRA']").click()
driver.get_screenshot_as_file("yahoo7.png")
time.sleep(9600)