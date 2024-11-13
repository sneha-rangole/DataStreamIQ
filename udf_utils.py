import re
from datetime import datetime

def extract_file_name(file_content):
    return file_content.strip().split('\n')[0]

def extract_position(file_content):
    return file_content.strip().split('\n')[0]

def extract_classcode(file_content):
    try:
        classcode_match = re.search(r'(Class Code: )\s+(\d+)', file_content)
        return classcode_match.group(2) if classcode_match else None
    except Exception as e:
        raise ValueError(f'Error extracting classcode: {e}')

def extract_salary(file_content):
    try:
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+)?'
        salary_match = re.search(salary_pattern, file_content)
        if salary_match:
            salary_start = float(salary_match.group(1).replace(',', ''))
            salary_end = float(salary_match.group(3).replace(',', '')) if salary_match.group(3) else float(salary_match.group(2).replace(',', ''))
            return salary_start, salary_end
        return None, None
    except Exception as e:
        raise ValueError(f'Error extracting salary: {str(e)}')

def extract_requirements(file_content):
    try:
        req_match = re.search(r'(REQUIREMENTS/MINIMUM QUALIFICATIONS)\s+(.+?)\s+(NOTES)', file_content, re.DOTALL)
        req = req_match.group(2) if req_match else None
        return req.splitlines() if req else []
    except Exception as e:
        raise ValueError(f'Error extracting requirements: {e}')

def extract_notes(file_content):
    try:
        notes_match = re.search(r'(NOTES)\s+(.+?)\s+(DUTIES)', file_content, re.DOTALL)
        notes = notes_match.group(2) if notes_match else None
        return notes.splitlines() if notes else []
    except Exception as e:
        raise ValueError(f'Error extracting notes: {e}')

def extract_duties(file_content):
    try:
        duties_match = re.search(r'(DUTIES)\s+(.+?)\s+(REQ)', file_content, re.DOTALL)
        duties = duties_match.group(2) if duties_match else None
        return duties.splitlines() if duties else []
    except Exception as e:
        raise ValueError(f'Error extracting duties: {e}')

def extract_start_date(file_content):
    try:
        opendate_match = re.search(r'(Open Date: )\s+(\d{2}-\d{2}-\d{2})', file_content)
        opendate = opendate_match.group(2) if opendate_match else None
        return datetime.strptime(opendate, '%m-%d-%y') if opendate else None
    except Exception as e:
        raise ValueError(f'Error extracting opendate: {e}')

def extract_end_date(file_content):
    try:
        enddate_match = re.search(r'Applications must be received by [A-Za-z]+, ([A-Za-z]+ \d{1,2}, \d{4})', file_content)
        enddate = enddate_match.group(1) if enddate_match else None
        
        return datetime.strptime(enddate, '%B %d, %Y') if enddate else None
    except Exception as e:
        raise ValueError(f'Error extracting enddate: {e}')


def extract_selection(file_content):
    try:
        selection_match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+', file_content)
        return [z[0] for z in selection_match] if selection_match else None
    except Exception as e:
        raise ValueError(f'Error extracting selection: {str(e)}')

def extract_education_length(file_content):
    try:
        education_pattern = r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)(\s|-)(years?)\s(college|university)'
        education_match = re.search(education_pattern, file_content)
        return education_match.group(1) if education_match else None
    except Exception as e:
        raise ValueError(f'Error extracting education_length: {str(e)}')

def extract_experience_length(file_content):
    try:
        experience_pattern = r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)\s(years?)\s(of\sfull(-s|\s)time)'
        experience_match = re.search(experience_pattern, file_content)
        return experience_match.group(1) if experience_match else None
    except Exception as e:
        raise ValueError(f'Error extracting experience_length: {str(e)}')

def extract_application_location(file_content):
    try:
        app_location_match = re.search(r'(Applications? will only be accepted on-?line)', file_content, re.IGNORECASE)
        return 'Online' if app_location_match else 'Mail or In Person'
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
