import java.util.Date;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import JsonDateSerializer;


public class MarketingData {

	private String jobName;
	private String jobStatus;
	private String jobType;
	private Date jobRunTime;
	
	
	public MarketingData(String jobName, String jobType, String jobStatus,
			Date jobRunTime) {
		this.jobName = jobName;
		this.jobType = jobType;
		this.jobStatus = jobStatus;
		this.jobRunTime = jobRunTime;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public String getJobType() {
		return jobType;
	}
	public void setJobType(String jobType) {
		this.jobType = jobType;
	}
	@JsonSerialize(using=JsonDateSerializer.class)
	public Date getJobRunTime() {
		return jobRunTime;
	}
	public void setJobRunTime(Date jobRunTime) {
		this.jobRunTime = jobRunTime;
	}
	public String getJobStatus() {
		return jobStatus;
	}
	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}
	@Override
	public String toString() {
		return "Contact [jobName=" + jobName + ", jobType=" + jobType
				+ ", jobStatus=" + jobStatus + ", jobRunTime=" + jobRunTime + "]";
	}
	
}
