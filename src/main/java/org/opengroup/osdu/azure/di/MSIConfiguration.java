package org.opengroup.osdu.azure.di;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * A configuration bean class Enable/Disable MSI (Managed Service Identity).
 * This class also checks for Workload Identity configuration as an alternative authentication method.
 */
@Configuration
@ConfigurationProperties(prefix = "azure.msi")
@Getter
@Setter
public class MSIConfiguration {
    private Boolean isEnabled = false;

    @Autowired
    private WorkloadIdentityConfiguration workloadIdentityConfiguration;

    /**
     * Checks if either MSI or Workload Identity authentication is enabled.
     * @return true if either MSI or Workload Identity is enabled, false otherwise
     */
    public Boolean getIsEnabled() {
        return isEnabled || workloadIdentityConfiguration.getIsEnabled();
    }
}
