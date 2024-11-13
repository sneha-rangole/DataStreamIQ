import re
from datetime import datetime

def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_position(file_content):
    return extract_file_name(file_content) 

def extract_classcode(file_content):
    try:
        classcode_match = re.search(r'(Class Code: )\s+(\d+)', file_content)
        classcode = classcode_match.group(2) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f'Error extracting classcode: {e}')

def extract_salary(file_content):
    try:
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+)?'
        salary_match = re.search(salary_pattern, file_content)
        if salary_match:
            salary_start = float(salary_match.group(1).replace(',', ''))
            salary_end = float(salary_match.group(3).replace(',', '')) if salary_match.group(3) else float(salary_match.group(2).replace(',', ''))
        else:
            salary_start, salary_end = None, None
        return salary_start, salary_end
    except Exception as e:
        raise ValueError(f'Error extracting salary: {str(e)}')

def extract_requirements(file_content):
    try:
        req_match = re.search(r'(REQUIREMENTS/MINIMUM QUALIFICATIONS)\s+(.+?)\s+(NOTES)', file_content, re.DOTALL)
        req = req_match.group(2) if req_match else None
        return req.split("\r") if req else []  # Handle None and split safely
    except Exception as e:
        raise ValueError(f'Error extracting requirements: {e}')

def extract_notes(file_content):
    try:
        notes_match = re.search(r'(NOTES)\s+(.+?)\s+(DUTIES)', file_content, re.DOTALL)
        notes = notes_match.group(2) if notes_match else None
        return notes.split("\r") if notes else []  # Handle None and split safely
    except Exception as e:
        raise ValueError(f'Error extracting notes: {e}')

def extract_duties(file_content):
    try:
        duties_match = re.search(r'(DUTIES):(.*?)(REQ[A-Z])', file_content, re.DOTALL)
        duties = duties_match.group(2).strip() if duties_match else None
        return duties
    except Exception as e:
        raise ValueError(f'Error extracting duties: {str(e)}')

def extract_start_date(file_content):
    try:
        opendate_match = re.search(r'(Open [Dd]ate:)\s+(\d\d-\d\d-\d\d)', file_content)
        start_date = datetime.strptime(opendate_match.group(2), '%m-%d-%y') if opendate_match else None
        return start_date
    except Exception as e:
        raise ValueError(f'Error extracting opendate: {e}')

def extract_end_date(file_content):
    try:
        # Correct the regex pattern: Remove the extra parenthesis
        enddate_match = re.search(r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s(\d{1,2},\s\d{4})', file_content)
        enddate = enddate_match.group(2) if enddate_match else None
        enddate = datetime.strptime(enddate, '%B %d, %Y') if enddate else None
        return enddate
    except Exception as e:
        raise ValueError(f'Error extracting enddate: {e}')

def extract_selection(file_content):
    try:
        selection_match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+', file_content)
        selections = [z[0] for z in selection_match] if selection_match else None
        return selections
    except Exception as e:
        raise ValueError(f'Error extracting selection: {str(e)}')

def extract_education_length(file_content):
    try:
        education_pattern = r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)(\s|-)(years?)\s(college|university)'
        education_match = re.search(education_pattern, file_content)
        education_length = education_match.group(1) if education_match else None
        return education_length
    except Exception as e:
        raise ValueError(f'Error extracting education_length: {str(e)}')

def extract_experience_length(file_content):
    try:
        experience_pattern = r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)\s(years?)\s(of\sfull(-s|\s)time)'
        experience_match = re.search(experience_pattern, file_content)
        experience_length = experience_match.group(1) if experience_match else None
        return experience_length
    except Exception as e:
        raise ValueError(f'Error extracting experience_length: {str(e)}')

def extract_application_location(file_content):
    try:
        app_location_match = re.search(r'(Applications? will only be accepted on-?line)', file_content, re.IGNORECASE)
        app_loc = 'Online' if app_location_match else 'Mail or In Person'
        return app_loc
    except Exception as e:
        raise ValueError(f'Error extracting application_location: {str(e)}')

# Empty Functions with Comments
def extract_school_type(file_content):
    # To be implemented: Define how to extract school type
    pass

def extract_job_type(file_content):
    # To be implemented: Define how to extract job type
    pass

def extract_requirments(file_content):
    # To be implemented: Define how to extract requirements
    pass

def extract_req(file_content):
    # To be implemented: Define how to extract any other requirements
    pass
