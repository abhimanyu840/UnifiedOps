import type { Location, SystemStatus, TimeRange } from '../../types';
import { BrandTitle } from './BrandTitle';
import { LiveBadge, type DownService } from './LiveBadge';
import { LocationPicker } from './LocationPicker';
import { RangePicker } from './RangePicker';
import { RefreshButton } from './RefreshButton';

interface Props {
  status: SystemStatus;
  selectedLocations: Location[];
  allLocations: Location[];
  onLocationsChange: (next: Location[]) => void;
  range: TimeRange;
  onRangeChange: (next: TimeRange) => void;
  refreshing: boolean;
  onRefresh: () => void;
  downServices?: DownService[];
}

export function Header({
  status,
  selectedLocations,
  allLocations,
  onLocationsChange,
  range,
  onRangeChange,
  refreshing,
  onRefresh,
  downServices,
}: Props) {
  return (
    <header className="header">
      <div className="header__left">
        <div className="brand-group">
          <img src="/wipro.png" alt="Wipro" className="brand brand--wipro" />
          <img src="/hdfc.png" alt="HDFC Bank" className="brand brand--hdfc" />
        </div>
      </div>

      <div className="header__center">
        <BrandTitle />
      </div>

      <div className="header__actions">
        <LiveBadge status={status} downServices={downServices} />
        <LocationPicker
          selected={selectedLocations}
          all={allLocations}
          onChange={onLocationsChange}
        />
        <RefreshButton busy={refreshing} onClick={onRefresh} />
        <RangePicker value={range} onChange={onRangeChange} />
      </div>
    </header>
  );
}
